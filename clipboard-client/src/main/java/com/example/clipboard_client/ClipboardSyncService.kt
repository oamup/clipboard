package com.example.clipboard_client

import android.app.*
import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.net.nsd.NsdManager
import android.net.nsd.NsdServiceInfo
import android.net.wifi.WifiManager
import android.os.Build
import android.os.IBinder
import androidx.core.app.NotificationCompat
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import org.json.JSONObject
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import java.net.Socket
import java.util.UUID

class ClipboardSyncService : Service() {

    private val serviceScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    private var nsdManager: NsdManager? = null
    private var discoveryListener: NsdManager.DiscoveryListener? = null
    private var multicastLock: WifiManager.MulticastLock? = null
    private var socket: Socket? = null
    private var writer: PrintWriter? = null

    private var isRunning = false
    private var isManualConnection = false

    private val processedIds = mutableListOf<String>()
    private val MAX_HISTORY_SIZE = 10

    companion object {
        private const val CHANNEL_ID = "clipboard_sync_channel"
        private const val NOTIFICATION_ID = 101
        private const val SERVICE_TYPE = "_localclip._tcp."

        const val ACTION_SYNC_OUTBOUND = "com.example.clipboard_client.SYNC_OUTBOUND"
        const val ACTION_CONNECT_MANUAL = "CONNECT_MANUAL"

        private val _connectionStatus = MutableStateFlow("Disconnected")
        val connectionStatus = _connectionStatus.asStateFlow()

        private val _lastError = MutableStateFlow<String?>(null)
        val lastError = _lastError.asStateFlow()
    }

    override fun onCreate() {
        super.onCreate()
        nsdManager = getSystemService(Context.NSD_SERVICE) as NsdManager
        createNotificationChannel()

        val wifiManager = applicationContext.getSystemService(Context.WIFI_SERVICE) as WifiManager
        multicastLock = wifiManager.createMulticastLock("mDnsLock").apply {
            setReferenceCounted(true)
            acquire()
        }
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        val action = intent?.action

        if (!isRunning) {
            isRunning = true

            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                var type = 0
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
                    type = ServiceInfo.FOREGROUND_SERVICE_TYPE_CONNECTED_DEVICE
                }
                startForeground(NOTIFICATION_ID, buildNotification("Service initialized"), type)
            } else {
                startForeground(NOTIFICATION_ID, buildNotification("Service initialized"))
            }
        }

        when (action) {
            ACTION_SYNC_OUTBOUND -> {
                syncLocalClipboardToPc()
            }
            ACTION_CONNECT_MANUAL -> {
                val ip = intent.getStringExtra("ip") ?: return START_STICKY
                val port = intent.getIntExtra("port", 8080)

                isManualConnection = true
                stopNsdDiscovery()
                _lastError.value = null
                connectToPc(ip, port)
            }
            else -> {
                if (!isManualConnection) {
                    startNsdDiscovery()
                }
            }
        }

        return START_STICKY
    }

    private fun startNsdDiscovery() {
        stopNsdDiscovery()

        _connectionStatus.value = "Searching for PC..."
        updateNotification("Searching for PC on local network...")

        val listener = object : NsdManager.DiscoveryListener {
            override fun onDiscoveryStarted(regType: String) {}

            override fun onServiceFound(serviceInfo: NsdServiceInfo) {
                if (serviceInfo.serviceType == SERVICE_TYPE) {
                    nsdManager?.resolveService(serviceInfo, object : NsdManager.ResolveListener {
                        override fun onResolveFailed(serviceInfo: NsdServiceInfo, errorCode: Int) {
                            _lastError.value = "mDNS Resolve Failed (Code: $errorCode)"
                        }

                        override fun onServiceResolved(serviceInfo: NsdServiceInfo) {
                            val hostIp = serviceInfo.host.hostAddress ?: return
                            val port = serviceInfo.port
                            connectToPc(hostIp, port)
                        }
                    })
                }
            }

            override fun onServiceLost(serviceInfo: NsdServiceInfo) {
                _connectionStatus.value = "PC mDNS connection lost"
                disconnectSocket()
            }

            override fun onDiscoveryStopped(regType: String) {}
            override fun onStartDiscoveryFailed(serviceType: String, errorCode: Int) {
                _lastError.value = "mDNS Start Failed (Code: $errorCode)"
                stopNsdDiscovery()
            }
            override fun onStopDiscoveryFailed(serviceType: String, errorCode: Int) {}
        }

        discoveryListener = listener

        try {

            nsdManager?.discoverServices(SERVICE_TYPE, NsdManager.PROTOCOL_DNS_SD, listener)
        } catch (e: Exception) {
            _lastError.value = "Discovery error: ${e.message}"
        }
    }

    private fun stopNsdDiscovery() {
        try {
            discoveryListener?.let { nsdManager?.stopServiceDiscovery(it) }
        } catch (e: Exception) {}
        discoveryListener = null
    }

    private fun connectToPc(host: String, port: Int) {
        serviceScope.launch {
            try {
                disconnectSocket()
                _connectionStatus.value = "Connecting to $host:$port..."
                updateNotification("Connecting to PC...")

                val newSocket = Socket(host, port)
                socket = newSocket
                writer = PrintWriter(newSocket.getOutputStream(), true)

                _connectionStatus.value = "Connected to PC"
                _lastError.value = null
                updateNotification("Connected to PC. Clipboard synchronization active.")

                val reader = BufferedReader(InputStreamReader(newSocket.getInputStream()))

                while (isRunning) {
                    val line = reader.readLine() ?: break
                    handleInboundPayload(line)
                }

                throw Exception("Connection closed by PC")

            } catch (e: Exception) {
                _connectionStatus.value = "Connection failed/dropped."
                _lastError.value = e.message
                updateNotification("Disconnected from PC.")

                delay(5000)

                if (isRunning) {
                    if (isManualConnection) {
                        connectToPc(host, port)
                    } else {
                        startNsdDiscovery()
                    }
                }
            }
        }
    }

    private fun disconnectSocket() {
        try { writer?.close(); socket?.close() } catch (e: Exception) {}
        writer = null
        socket = null
    }

    private fun handleInboundPayload(rawJson: String?) {
        if (rawJson == null) return
        try {
            val json = JSONObject(rawJson)
            val id = json.getString("id")
            val text = json.getString("text")

            if (processedIds.contains(id)) return

            processedIds.add(id)
            if (processedIds.size > MAX_HISTORY_SIZE) processedIds.removeAt(0)

            serviceScope.launch(Dispatchers.Main) {
                val clipboard = getSystemService(Context.CLIPBOARD_SERVICE) as ClipboardManager

                val currentClip = clipboard.primaryClip
                if (currentClip != null && currentClip.itemCount > 0) {
                    if (currentClip.getItemAt(0).text?.toString() == text) return@launch
                }

                val clip = ClipData.newPlainText("synced_data", text)
                clipboard.setPrimaryClip(clip)
            }
        } catch (e: Exception) {
            _lastError.value = "JSON Parse Error: ${e.message}"
        }
    }

    private fun syncLocalClipboardToPc() {
        serviceScope.launch(Dispatchers.Main) {
            val clipboard = getSystemService(Context.CLIPBOARD_SERVICE) as ClipboardManager
            val clipData = clipboard.primaryClip

            if (clipData != null && clipData.itemCount > 0) {
                val text = clipData.getItemAt(0).text?.toString() ?: ""

                if (text.isNotEmpty()) {
                    val uniqueId = UUID.randomUUID().toString()

                    processedIds.add(uniqueId)
                    if (processedIds.size > MAX_HISTORY_SIZE) processedIds.removeAt(0)

                    val payload = JSONObject().apply {
                        put("id", uniqueId)
                        put("text", text)
                    }.toString()

                    withContext(Dispatchers.IO) {
                        writer?.println(payload)
                    }
                }
            }
        }
    }

    private fun createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val serviceChannel = NotificationChannel(
                CHANNEL_ID, "Clipboard Sync Background Worker",
                NotificationManager.IMPORTANCE_LOW
            ).apply {
                description = "Maintains local network connection to PC"
            }
            val manager = getSystemService(NotificationManager::class.java)
            manager?.createNotificationChannel(serviceChannel)
        }
    }

    private fun buildNotification(contentText: String): Notification {
        val notificationIntent = Intent(this, MainActivity::class.java)
        val pendingIntent = PendingIntent.getActivity(
            this, 0, notificationIntent,
            PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT
        )

        val syncIntent = Intent(this, ClipboardSyncService::class.java).apply {
            action = ACTION_SYNC_OUTBOUND
        }
        val syncPendingIntent = PendingIntent.getService(
            this, 1, syncIntent,
            PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT
        )

        return NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle("Clipboard Local Sync")
            .setContentText(contentText)
            .setSmallIcon(android.R.drawable.ic_menu_share)
            .setContentIntent(pendingIntent)
            .addAction(android.R.drawable.ic_menu_upload, "Sync Local Clipboard", syncPendingIntent)
            .setOngoing(true)
            .build()
    }

    private fun updateNotification(text: String) {
        val notificationManager = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        notificationManager.notify(NOTIFICATION_ID, buildNotification(text))
    }

    override fun onDestroy() {
        isRunning = false
        disconnectSocket()
        stopNsdDiscovery()
        multicastLock?.takeIf { it.isHeld }?.release()
        serviceScope.cancel()
        super.onDestroy()
    }

    override fun onBind(intent: Intent?): IBinder? = null
}


