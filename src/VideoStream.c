#include "Limelight-internal.h"

#define FIRST_FRAME_MAX 1500
#define FIRST_FRAME_TIMEOUT_SEC 10

#define FIRST_FRAME_PORT 47996

static RTP_VIDEO_QUEUE rtpQueue;

static SOCKET rtpSocket = INVALID_SOCKET;
static SOCKET firstFrameSocket = INVALID_SOCKET;

static PPLT_CRYPTO_CONTEXT decryptionCtx;

static PLT_THREAD udpPingThread;
static PLT_THREAD receiveThread;
static PLT_THREAD decoderThread;

static bool receivedDataFromPeer;
static uint64_t firstDataTimeMs;
static bool receivedFullFrame;

// We can't request an IDR frame until the depacketizer knows
// that a packet was lost. This timeout bounds the time that
// the RTP queue will wait for missing/reordered packets.
#define RTP_QUEUE_DELAY 10

// This is the desired number of video packets that can be
// stored in the socket's receive buffer. 2048 is chosen
// because it should be large enough for all reasonable
// frame sizes (probably 2 or 3 frames) without using too
// much kernel memory with larger packet sizes. It also
// can smooth over transient pauses in network traffic
// and subsequent packet/frame bursts that follow.
#define RTP_RECV_PACKETS_BUFFERED 2048

// Adaptive bitrate (Apollo fork extension, host advertises SS_FF_AP_ADAPTIVE_BITRATE).
//
// Delay-based congestion detection: for every frame we take the arrival time of its
// last packet minus its capture time (RTP timestamp). The unknown clock offset is
// removed by subtracting the minimum over the last ~30 s, which leaves the queuing
// delay along the path. Single delay spikes (common on mobile links) are ignored by
// only looking at the smallest delay within each 250 ms window: if even that one is
// high, a queue is standing in the path. Then the bitrate is cut (multiplicative
// decrease); while the path is calm it is raised slowly (additive increase) up to the
// configured bitrate. The requested bitrate is sent to the host by the control stream
// thread (see abrGetRequestedBitrate()).
#define ABR_BUCKET_US           5000000   // min-delay bucket length
#define ABR_BUCKETS             6         // -> baseline over the last 30 s
#define ABR_WINDOW_US           250000    // evaluation window (standing queue = min delay in window)
#define ABR_RATE_WINDOW_US      500000    // receive rate measurement window
#define ABR_OVERUSE_MS          30.0      // standing queue that counts as congestion
#define ABR_NORMAL_MS           15.0      // below this the path is considered calm
#define ABR_DECREASE_FACTOR     0.85
#define ABR_DECREASE_HOLD_US    1000000   // minimum time between two decreases (let the queue drain)
#define ABR_INCREASE_DELAY_US   2000000   // no increase for this long after a decrease
#define ABR_INCREASE_PER_SEC    0.05      // + 5 % of the configured bitrate per second while calm
#define ABR_MIN_KBPS            1000
#define ABR_LOG_INTERVAL_US     5000000

static struct {
    bool enabled;
    bool haveFrame;
    uint32_t firstTs;
    uint32_t curTs;
    uint64_t curLastArrivalUs;

    int64_t bucketMin[ABR_BUCKETS];
    int bucketIndex;
    uint64_t bucketStartUs;

    uint64_t windowStartUs;
    int64_t windowMinDelayUs;
    double queueDelayMs;

    uint64_t rateBytes;
    uint64_t rateWindowStartUs;
    double recvKbps;

    double targetKbps;
    uint64_t lastDecreaseUs;
    uint64_t lastUpdateUs;
    uint64_t lastLogUs;
    volatile uint32_t requestedKbps;
} abr;

static void abrInitialize(void) {
    memset(&abr, 0, sizeof(abr));
    abr.enabled = IS_SUNSHINE() && (SunshineFeatureFlags & SS_FF_AP_ADAPTIVE_BITRATE) && StreamConfig.bitrate > 0;
    abr.targetKbps = StreamConfig.bitrate;
    abr.windowMinDelayUs = INT64_MAX;
    for (int i = 0; i < ABR_BUCKETS; i++) {
        abr.bucketMin[i] = INT64_MAX;
    }
    if (abr.enabled) {
        Limelog("ABR: adaptive bitrate enabled (max %d kbps)\n", StreamConfig.bitrate);
    }
}

uint32_t abrGetRequestedBitrate(void) {
    return abr.requestedKbps;
}

// Called once per evaluation window with the standing queuing delay
static void abrEvaluate(uint64_t nowUs) {
    double maxKbps = StreamConfig.bitrate;
    double newTarget = abr.targetKbps;

    if (abr.queueDelayMs > ABR_OVERUSE_MS) {
        if (nowUs - abr.lastDecreaseUs >= ABR_DECREASE_HOLD_US) {
            // If the stream actually uses its bitrate, the receive rate tells what got through.
            // With little motion the encoder stays far below the target, then just step down.
            if (abr.recvKbps >= 0.5 * abr.targetKbps) {
                newTarget = ABR_DECREASE_FACTOR * abr.recvKbps;
            }
            if (newTarget > ABR_DECREASE_FACTOR * abr.targetKbps) {
                newTarget = ABR_DECREASE_FACTOR * abr.targetKbps;
            }
            abr.lastDecreaseUs = nowUs;
            Limelog("ABR: congestion (queue %.1f ms, recv %.0f kbps) -> %.0f kbps\n",
                    abr.queueDelayMs, abr.recvKbps, newTarget < ABR_MIN_KBPS ? ABR_MIN_KBPS : newTarget);
        }
    }
    else if (abr.queueDelayMs < ABR_NORMAL_MS && nowUs - abr.lastDecreaseUs >= ABR_INCREASE_DELAY_US) {
        if (abr.lastUpdateUs != 0) {
            double dt = (nowUs - abr.lastUpdateUs) / 1000000.0;
            newTarget += maxKbps * ABR_INCREASE_PER_SEC * dt;
        }
    }
    abr.lastUpdateUs = nowUs;

    if (newTarget > maxKbps) {
        newTarget = maxKbps;
    }
    if (newTarget < ABR_MIN_KBPS) {
        newTarget = ABR_MIN_KBPS;
    }
    abr.targetKbps = newTarget;

    // Only signal changes of at least 3 %, plus the final step back to the maximum
    uint32_t current = abr.requestedKbps ? abr.requestedKbps : (uint32_t)maxKbps;
    if (newTarget < current * 0.97 || newTarget > current * 1.03 ||
        (newTarget == maxKbps && current != (uint32_t)maxKbps)) {
        abr.requestedKbps = (uint32_t)newTarget;
    }

    if (nowUs - abr.lastLogUs >= ABR_LOG_INTERVAL_US) {
        abr.lastLogUs = nowUs;
        Limelog("ABR: queue %.1f ms, recv %.0f kbps, target %.0f kbps\n",
                abr.queueDelayMs, abr.recvKbps, abr.targetKbps);
    }
}

static void abrOnFrame(uint32_t rtpTimestamp, uint64_t arrivalUs) {
    // RTP video timestamps use a 90 kHz clock based on the host's capture time
    uint64_t captureUs = (uint64_t)(uint32_t)(rtpTimestamp - abr.firstTs) * 1000 / 90;
    int64_t delayUs = (int64_t)arrivalUs - (int64_t)captureUs;

    if (abr.bucketStartUs == 0) {
        abr.bucketStartUs = arrivalUs;
    }
    else if (arrivalUs - abr.bucketStartUs >= ABR_BUCKET_US) {
        abr.bucketIndex = (abr.bucketIndex + 1) % ABR_BUCKETS;
        abr.bucketMin[abr.bucketIndex] = INT64_MAX;
        abr.bucketStartUs = arrivalUs;
    }
    if (delayUs < abr.bucketMin[abr.bucketIndex]) {
        abr.bucketMin[abr.bucketIndex] = delayUs;
    }

    if (abr.windowStartUs == 0) {
        abr.windowStartUs = arrivalUs;
    }
    if (delayUs < abr.windowMinDelayUs) {
        abr.windowMinDelayUs = delayUs;
    }

    if (arrivalUs - abr.windowStartUs >= ABR_WINDOW_US) {
        int64_t baseline = INT64_MAX;
        for (int i = 0; i < ABR_BUCKETS; i++) {
            if (abr.bucketMin[i] < baseline) {
                baseline = abr.bucketMin[i];
            }
        }

        abr.queueDelayMs = (abr.windowMinDelayUs - baseline) / 1000.0;
        abr.windowMinDelayUs = INT64_MAX;
        abr.windowStartUs = arrivalUs;

        abrEvaluate(arrivalUs);
    }
}

static void abrOnPacket(uint32_t rtpTimestamp, int length) {
    if (!abr.enabled) {
        return;
    }

    uint64_t now = PltGetMicroseconds();

    abr.rateBytes += length;
    if (abr.rateWindowStartUs == 0) {
        abr.rateWindowStartUs = now;
    }
    else if (now - abr.rateWindowStartUs >= ABR_RATE_WINDOW_US) {
        abr.recvKbps = abr.rateBytes * 8.0 / ((now - abr.rateWindowStartUs) / 1000.0);
        abr.rateBytes = 0;
        abr.rateWindowStartUs = now;
    }

    if (!abr.haveFrame) {
        abr.haveFrame = true;
        abr.firstTs = rtpTimestamp;
        abr.curTs = rtpTimestamp;
        abr.curLastArrivalUs = now;
        return;
    }

    if (rtpTimestamp == abr.curTs) {
        abr.curLastArrivalUs = now;
    }
    else if ((int32_t)(rtpTimestamp - abr.curTs) > 0) {
        // First packet of a newer frame: the previous frame is complete
        abrOnFrame(abr.curTs, abr.curLastArrivalUs);
        abr.curTs = rtpTimestamp;
        abr.curLastArrivalUs = now;
    }
    // Packets of older frames (reordering) are ignored
}

// Initialize the video stream
void initializeVideoStream(void) {
    initializeVideoDepacketizer(StreamConfig.packetSize);
    RtpvInitializeQueue(&rtpQueue);
    decryptionCtx = PltCreateCryptoContext();
    receivedDataFromPeer = false;
    firstDataTimeMs = 0;
    receivedFullFrame = false;
}

// Clean up the video stream
void destroyVideoStream(void) {
    PltDestroyCryptoContext(decryptionCtx);
    destroyVideoDepacketizer();
    RtpvCleanupQueue(&rtpQueue);
}

// UDP Ping proc
static void VideoPingThreadProc(void* context) {
    char legacyPingData[] = { 0x50, 0x49, 0x4E, 0x47 };
    LC_SOCKADDR saddr;

    LC_ASSERT(VideoPortNumber != 0);

    memcpy(&saddr, &RemoteAddr, sizeof(saddr));
    SET_PORT(&saddr, VideoPortNumber);

    // We do not check for errors here. Socket errors will be handled
    // on the read-side in ReceiveThreadProc(). This avoids potential
    // issues related to receiving ICMP port unreachable messages due
    // to sending a packet prior to the host PC binding to that port.
    int pingCount = 0;
    while (!PltIsThreadInterrupted(&udpPingThread)) {
        if (VideoPingPayload.payload[0] != 0) {
            pingCount++;
            VideoPingPayload.sequenceNumber = BE32(pingCount);

            sendto(rtpSocket, (char*)&VideoPingPayload, sizeof(VideoPingPayload), 0, (struct sockaddr*)&saddr, AddrLen);
        }
        else {
            sendto(rtpSocket, legacyPingData, sizeof(legacyPingData), 0, (struct sockaddr*)&saddr, AddrLen);
        }

        PltSleepMsInterruptible(&udpPingThread, 500);
    }
}

// Receive thread proc
static void VideoReceiveThreadProc(void* context) {
    int err;
    int bufferSize, receiveSize, decryptedSize, minSize;
    char* buffer;
    char* encryptedBuffer;
    int queueStatus;
    bool useSelect;
    int waitingForVideoMs;
    bool encrypted;

    encrypted = !!(EncryptionFeaturesEnabled & SS_ENC_VIDEO);
    decryptedSize = StreamConfig.packetSize + MAX_RTP_HEADER_SIZE;
    minSize = sizeof(RTP_PACKET) + ((EncryptionFeaturesEnabled & SS_ENC_VIDEO) ? sizeof(ENC_VIDEO_HEADER) : 0);
    receiveSize = decryptedSize + ((EncryptionFeaturesEnabled & SS_ENC_VIDEO) ? sizeof(ENC_VIDEO_HEADER) : 0);
    bufferSize = decryptedSize + sizeof(RTPV_QUEUE_ENTRY);
    buffer = NULL;

    if (setNonFatalRecvTimeoutMs(rtpSocket, UDP_RECV_POLL_TIMEOUT_MS) < 0) {
        // SO_RCVTIMEO failed, so use select() to wait
        useSelect = true;
    }
    else {
        // SO_RCVTIMEO timeout set for recv()
        useSelect = false;
    }

    // Allocate a staging buffer to use for each received packet
    if (encrypted) {
        encryptedBuffer = (char*)malloc(receiveSize);
        if (encryptedBuffer == NULL) {
            Limelog("Video Receive: malloc() failed\n");
            ListenerCallbacks.connectionTerminated(-1);
            return;
        }
    }
    else {
        encryptedBuffer = NULL;
    }

    waitingForVideoMs = 0;
    while (!PltIsThreadInterrupted(&receiveThread)) {
        PRTP_PACKET packet;

        if (buffer == NULL) {
            buffer = (char*)malloc(bufferSize);
            if (buffer == NULL) {
                Limelog("Video Receive: malloc() failed\n");
                ListenerCallbacks.connectionTerminated(-1);
                break;
            }
        }

        err = recvUdpSocket(rtpSocket,
                            encrypted ? encryptedBuffer : buffer,
                            receiveSize,
                            useSelect);
        if (err < 0) {
            Limelog("Video Receive: recvUdpSocket() failed: %d\n", (int)LastSocketError());
            ListenerCallbacks.connectionTerminated(LastSocketFail());
            break;
        }
        else if  (err == 0) {
            if (!receivedDataFromPeer) {
                // If we wait many seconds without ever receiving a video packet,
                // assume something is broken and terminate the connection.
                waitingForVideoMs += UDP_RECV_POLL_TIMEOUT_MS;
                if (waitingForVideoMs >= FIRST_FRAME_TIMEOUT_SEC * 1000) {
                    Limelog("Terminating connection due to lack of video traffic\n");
                    ListenerCallbacks.connectionTerminated(ML_ERROR_NO_VIDEO_TRAFFIC);
                    break;
                }
            }

            // Receive timed out; try again
            continue;
        }

        if (!receivedDataFromPeer) {
            receivedDataFromPeer = true;
            Limelog("Received first video packet after %d ms\n", waitingForVideoMs);

            firstDataTimeMs = PltGetMillis();
        }

#ifndef LC_FUZZING
        if (!receivedFullFrame) {
            if (PltGetMillis() - firstDataTimeMs >= FIRST_FRAME_TIMEOUT_SEC * 1000) {
                Limelog("Terminating connection due to lack of a successful video frame\n");
                ListenerCallbacks.connectionTerminated(ML_ERROR_NO_VIDEO_FRAME);
                break;
            }
        }
#endif

        if (err < minSize) {
            // Runt packet
            continue;
        }

        // Decrypt the packet into the buffer if encryption is enabled
        if (encrypted) {
            PENC_VIDEO_HEADER encHeader = (PENC_VIDEO_HEADER)encryptedBuffer;

            // If this frame is below our current frame number, discard it before decryption
            // to save CPU cycles decrypting FEC shards for a frame we already reassembled.
            //
            // Since this is happening _before_ decryption, this packet is not trusted yet.
            // It's imperative that we do not mutate any state based on this packet until
            // after it has been decrypted successfully!
            //
            // It's possible for an attacker to inject a fake packet that has any value of
            // header fields they want, however this provides them no benefit because we will
            // simply drop said packet here (if it's below the current frame number) or it
            // will pass this check and be dropped during decryption (if contents is tampered)
            // or after decryption in the RTP queue (if it's a replay of a previous authentic
            // packet from the host).
            //
            // In short, an attacker spoofing this value via MITM or sending malicious values
            // impersonating the host from off-link doesn't gain them anything. If they have
            // a true MITM, they can DoS our connection by just dropping all our traffic, so
            // tampering with packets to fail this check doesn't accomplish anything they
            // couldn't already do. If they're not on-link, we just throw their malicious
            // traffic away (as mentioned in the paragraph above) and continue accepting
            // legitmate video traffic.
            if (encHeader->frameNumber && LE32(encHeader->frameNumber) < RtpvGetCurrentFrameNumber(&rtpQueue)) {
                continue;
            }

            if (!PltDecryptMessage(decryptionCtx, ALGORITHM_AES_GCM, 0,
                                   (unsigned char*)StreamConfig.remoteInputAesKey, sizeof(StreamConfig.remoteInputAesKey),
                                   encHeader->iv, sizeof(encHeader->iv),
                                   encHeader->tag, sizeof(encHeader->tag),
                                   ((unsigned char*)(encHeader + 1)), err - sizeof(ENC_VIDEO_HEADER), // The ciphertext is after the header
                                   (unsigned char*)buffer, &err)) {
                Limelog("Failed to decrypt video packet!\n");
                continue;
            }
        }

        // Convert fields to host byte-order
        packet = (PRTP_PACKET)&buffer[0];
        packet->sequenceNumber = BE16(packet->sequenceNumber);
        packet->timestamp = BE32(packet->timestamp);
        packet->ssrc = BE32(packet->ssrc);

        abrOnPacket(packet->timestamp, err);

        queueStatus = RtpvAddPacket(&rtpQueue, packet, err, (PRTPV_QUEUE_ENTRY)&buffer[decryptedSize]);

        if (queueStatus == RTPF_RET_QUEUED) {
            // The queue owns the buffer
            buffer = NULL;
        }
    }

    if (buffer != NULL) {
        free(buffer);
    }

    if (encryptedBuffer != NULL) {
        free(encryptedBuffer);
    }
}

void notifyKeyFrameReceived(void) {
    // Remember that we got a full frame successfully
    receivedFullFrame = true;
}

// Decoder thread proc
static void VideoDecoderThreadProc(void* context) {
    while (!PltIsThreadInterrupted(&decoderThread)) {
        VIDEO_FRAME_HANDLE frameHandle;
        PDECODE_UNIT decodeUnit;

        if (!LiWaitForNextVideoFrame(&frameHandle, &decodeUnit)) {
            return;
        }

        LiCompleteVideoFrame(frameHandle, VideoCallbacks.submitDecodeUnit(decodeUnit));
    }
}

// Read the first frame of the video stream
int readFirstFrame(void) {
    // All that matters is that we close this socket.
    // This starts the flow of video on Gen 3 servers.

    closeSocket(firstFrameSocket);
    firstFrameSocket = INVALID_SOCKET;

    return 0;
}

// Terminate the video stream
void stopVideoStream(void) {
    if (!receivedDataFromPeer) {
        Limelog("No video traffic was ever received from the host!\n");
    }

    VideoCallbacks.stop();

    // Wake up client code that may be waiting on the decode unit queue
    stopVideoDepacketizer();

    PltInterruptThread(&udpPingThread);
    PltInterruptThread(&receiveThread);
    if ((VideoCallbacks.capabilities & (CAPABILITY_DIRECT_SUBMIT | CAPABILITY_PULL_RENDERER)) == 0) {
        PltInterruptThread(&decoderThread);
    }

    if (firstFrameSocket != INVALID_SOCKET) {
        shutdownTcpSocket(firstFrameSocket);
    }

    PltJoinThread(&udpPingThread);
    PltJoinThread(&receiveThread);
    if ((VideoCallbacks.capabilities & (CAPABILITY_DIRECT_SUBMIT | CAPABILITY_PULL_RENDERER)) == 0) {
        PltJoinThread(&decoderThread);
    }

    if (firstFrameSocket != INVALID_SOCKET) {
        closeSocket(firstFrameSocket);
        firstFrameSocket = INVALID_SOCKET;
    }
    if (rtpSocket != INVALID_SOCKET) {
        closeSocket(rtpSocket);
        rtpSocket = INVALID_SOCKET;
    }

    VideoCallbacks.cleanup();
}

// Start the video stream
int startVideoStream(void* rendererContext, int drFlags) {
    int err;

    firstFrameSocket = INVALID_SOCKET;

    // RTSP negotiation is done at this point, so the host's feature flags are known
    abrInitialize();

    // This must be called before the decoder thread starts submitting
    // decode units
    LC_ASSERT(NegotiatedVideoFormat != 0);
    err = VideoCallbacks.setup(NegotiatedVideoFormat, StreamConfig.width,
        StreamConfig.height, StreamConfig.fps, rendererContext, drFlags);
    if (err != 0) {
        return err;
    }

    rtpSocket = bindUdpSocket(RemoteAddr.ss_family, &LocalAddr, AddrLen,
                              RTP_RECV_PACKETS_BUFFERED * (StreamConfig.packetSize + MAX_RTP_HEADER_SIZE),
                              SOCK_QOS_TYPE_VIDEO);
    if (rtpSocket == INVALID_SOCKET) {
        VideoCallbacks.cleanup();
        return LastSocketError();
    }

    VideoCallbacks.start();

    err = PltCreateThread("VideoRecv", VideoReceiveThreadProc, NULL, &receiveThread);
    if (err != 0) {
        VideoCallbacks.stop();
        closeSocket(rtpSocket);
        VideoCallbacks.cleanup();
        return err;
    }

    if ((VideoCallbacks.capabilities & (CAPABILITY_DIRECT_SUBMIT | CAPABILITY_PULL_RENDERER)) == 0) {
        err = PltCreateThread("VideoDec", VideoDecoderThreadProc, NULL, &decoderThread);
        if (err != 0) {
            VideoCallbacks.stop();
            PltInterruptThread(&receiveThread);
            PltJoinThread(&receiveThread);
            closeSocket(rtpSocket);
            VideoCallbacks.cleanup();
            return err;
        }
    }

    if (AppVersionQuad[0] == 3) {
        // Connect this socket to open port 47998 for our ping thread
        firstFrameSocket = connectTcpSocket(&RemoteAddr, AddrLen,
                                            FIRST_FRAME_PORT, FIRST_FRAME_TIMEOUT_SEC);
        if (firstFrameSocket == INVALID_SOCKET) {
            VideoCallbacks.stop();
            stopVideoDepacketizer();
            PltInterruptThread(&receiveThread);
            if ((VideoCallbacks.capabilities & (CAPABILITY_DIRECT_SUBMIT | CAPABILITY_PULL_RENDERER)) == 0) {
                PltInterruptThread(&decoderThread);
            }
            PltJoinThread(&receiveThread);
            if ((VideoCallbacks.capabilities & (CAPABILITY_DIRECT_SUBMIT | CAPABILITY_PULL_RENDERER)) == 0) {
                PltJoinThread(&decoderThread);
            }
            closeSocket(rtpSocket);
            VideoCallbacks.cleanup();
            return LastSocketError();
        }
    }

    // Start pinging before reading the first frame so GFE knows where
    // to send UDP data
    err = PltCreateThread("VideoPing", VideoPingThreadProc, NULL, &udpPingThread);
    if (err != 0) {
        VideoCallbacks.stop();
        stopVideoDepacketizer();
        PltInterruptThread(&receiveThread);
        if ((VideoCallbacks.capabilities & (CAPABILITY_DIRECT_SUBMIT | CAPABILITY_PULL_RENDERER)) == 0) {
            PltInterruptThread(&decoderThread);
        }
        PltJoinThread(&receiveThread);
        if ((VideoCallbacks.capabilities & (CAPABILITY_DIRECT_SUBMIT | CAPABILITY_PULL_RENDERER)) == 0) {
            PltJoinThread(&decoderThread);
        }
        closeSocket(rtpSocket);
        if (firstFrameSocket != INVALID_SOCKET) {
            closeSocket(firstFrameSocket);
            firstFrameSocket = INVALID_SOCKET;
        }
        VideoCallbacks.cleanup();
        return err;
    }

    if (AppVersionQuad[0] == 3) {
        // Read the first frame to start the flow of video
        err = readFirstFrame();
        if (err != 0) {
            stopVideoStream();
            return err;
        }
    }

    return 0;
}

const RTP_VIDEO_STATS* LiGetRTPVideoStats(void) {
    return &rtpQueue.stats;
}
