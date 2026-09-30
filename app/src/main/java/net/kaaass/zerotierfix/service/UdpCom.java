package net.kaaass.zerotierfix.service;

import android.util.Log;

import com.zerotier.sdk.Node;
import com.zerotier.sdk.PacketSender;
import com.zerotier.sdk.ResultCode;

import net.kaaass.zerotierfix.util.DebugLog;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

// TODO: clear up
public class UdpCom implements PacketSender, Runnable {
    private static final String TAG = "UdpCom";

    private static final class TxState {
        final DatagramSocket socket;
        final long generation;

        TxState(DatagramSocket socket, long generation) {
            this.socket = socket;
            this.generation = generation;
        }
    }

    private Node node;
    private volatile TxState txState;
    private final ZeroTierOneService ztService;
    private final AtomicLong txAttempts = new AtomicLong();
    private final AtomicLong txSuccess = new AtomicLong();
    private final AtomicLong txErrors = new AtomicLong();
    private final AtomicLong txSuspended = new AtomicLong();
    private final AtomicLong rxPackets = new AtomicLong();
    private final AtomicLong rxErrors = new AtomicLong();

    UdpCom(ZeroTierOneService zeroTierOneService, DatagramSocket datagramSocket) {
        this.txState = new TxState(datagramSocket, 1);
        this.ztService = zeroTierOneService;
    }

    public void setNode(Node node2) {
        this.node = node2;
    }

    public synchronized boolean suspendTx(DatagramSocket expectedSocket, long generation) {
        TxState state = this.txState;
        if (state.socket != expectedSocket) {
            Log.w(TAG, "UDP TX suspend rejected: expected socket is not current, current generation="
                    + state.generation);
            return false;
        }
        this.txState = new TxState(null, generation);
        Log.i(TAG, "UDP TX suspended: generation=" + generation);
        return true;
    }

    public void installTxSocket(DatagramSocket socket, long generation) {
        this.txState = new TxState(socket, generation);
        Log.i(TAG, "UDP TX socket installed: generation=" + generation
                + " local=" + localEndpoint(socket));
    }

    public Runnable receiverRunnable(DatagramSocket socket, long generation,
                                     CountDownLatch publishGate, CountDownLatch readyLatch) {
        return () -> receiveLoop(socket, generation, publishGate, readyLatch);
    }

    public void logStats(String reason) {
        TxState state = this.txState;
        Log.i(TAG, "UDP_STATS reason=" + reason
                + " generation=" + state.generation
                + " txLocal=" + localEndpoint(state.socket)
                + " txAttempts=" + this.txAttempts.get()
                + " txSuccess=" + this.txSuccess.get()
                + " txErrors=" + this.txErrors.get()
                + " txSuspended=" + this.txSuspended.get()
                + " rxPackets=" + this.rxPackets.get()
                + " rxErrors=" + this.rxErrors.get());
    }

    private static String localEndpoint(DatagramSocket socket) {
        if (socket == null) {
            return "null";
        }
        try {
            return String.valueOf(socket.getLocalSocketAddress()) + " closed=" + socket.isClosed();
        } catch (Exception e) {
            return "unavailable:" + e.getClass().getSimpleName();
        }
    }

    @Override
    public int onSendPacketRequested(long j, InetSocketAddress inetSocketAddress, byte[] bArr, int i) {
        TxState state = this.txState;
        DatagramSocket socket = state.socket;
        this.txAttempts.incrementAndGet();
        if (socket == null || socket.isClosed()) {
            this.txSuspended.incrementAndGet();
            return -1;
        }
        try {
            DatagramPacket datagramPacket = new DatagramPacket(bArr, bArr.length, inetSocketAddress);
            socket.send(datagramPacket);
            this.txSuccess.incrementAndGet();
            DebugLog.d(TAG, "onSendPacketRequested: Sent " + datagramPacket.getLength()
                    + " bytes to " + inetSocketAddress + " generation=" + state.generation);
            return 0;
        } catch (Exception e) {
            this.txErrors.incrementAndGet();
            Log.w(TAG, "UDP send failed: generation=" + state.generation
                    + " destination=" + inetSocketAddress + " error=" + e);
            return -1;
        }
    }

    @Override
    public void run() {
        TxState state = this.txState;
        receiveLoop(state.socket, state.generation, null, null);
    }

    private void receiveLoop(DatagramSocket rxSocket, long generation,
                             CountDownLatch publishGate, CountDownLatch readyLatch) {
        boolean readySignalled = false;
        try {
            if (rxSocket == null || rxSocket.isClosed()) {
                Log.e(TAG, "UDP RX cannot start without usable socket: generation=" + generation);
                return;
            }
            if (publishGate != null) {
                publishGate.await();
            }
            if (Thread.currentThread().isInterrupted() || rxSocket.isClosed()) {
                return;
            }
            Log.i(TAG, "UDP RX started: generation=" + generation + " local=" + localEndpoint(rxSocket));
            if (readyLatch != null) {
                readyLatch.countDown();
                readySignalled = true;
            }
            long[] deadline = new long[1];
            byte[] buffer = new byte[16384];
            while (!Thread.currentThread().isInterrupted()) {
                deadline[0] = 0;
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                try {
                    rxSocket.receive(packet);
                    if (packet.getLength() > 0) {
                        this.rxPackets.incrementAndGet();
                        byte[] payload = new byte[packet.getLength()];
                        System.arraycopy(packet.getData(), 0, payload, 0, packet.getLength());
                        DebugLog.d(TAG, "Got " + packet.getLength() + " Bytes From: "
                                + packet.getAddress() + ":" + packet.getPort()
                                + " generation=" + generation);
                        ResultCode result = this.node.processWirePacket(System.currentTimeMillis(), -1,
                                new InetSocketAddress(packet.getAddress(), packet.getPort()), payload, deadline);
                        if (result != ResultCode.RESULT_OK) {
                            Log.e(TAG, "processWirePacket returned: " + result);
                            this.ztService.shutdown();
                        }
                        this.ztService.setNextBackgroundTaskDeadline(deadline[0]);
                    }
                } catch (SocketTimeoutException ignored) {
                } catch (SocketException e) {
                    if (rxSocket.isClosed()) {
                        Log.i(TAG, "UDP RX socket closed: generation=" + generation);
                        break;
                    }
                    this.rxErrors.incrementAndGet();
                    Log.e(TAG, "UDP RX socket error: generation=" + generation + " error=" + e, e);
                    break;
                } catch (Exception e) {
                    this.rxErrors.incrementAndGet();
                    Log.e(TAG, "UDP RX error: generation=" + generation + " error=" + e, e);
                    break;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (readyLatch != null && !readySignalled) {
                readyLatch.countDown();
            }
            Log.i(TAG, "UDP RX ended: generation=" + generation);
        }
    }
}
