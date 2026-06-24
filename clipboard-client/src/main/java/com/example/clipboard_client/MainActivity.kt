package com.example.clipboard_client

import android.Manifest
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import androidx.core.content.ContextCompat

class MainActivity : ComponentActivity() {

    private val requestPermissionLauncher = registerForActivityResult(
        ActivityResultContracts.RequestPermission()
    ) { _ ->
        startSyncService()
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            if (ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS) == PackageManager.PERMISSION_GRANTED) {
                startSyncService()
            } else {
                requestPermissionLauncher.launch(Manifest.permission.POST_NOTIFICATIONS)
            }
        } else {
            startSyncService()
        }

        setContent {
            MaterialTheme {
                Surface(
                    modifier = Modifier.fillMaxSize(),
                    color = MaterialTheme.colorScheme.background
                ) {
                    val status by ClipboardSyncService.connectionStatus.collectAsState()
                    val error by ClipboardSyncService.lastError.collectAsState()

                    MainScreen(
                        status = status,
                        error = error,
                        onManualConnect = { ip, port ->
                            val intent = Intent(this, ClipboardSyncService::class.java).apply {
                                action = ClipboardSyncService.ACTION_CONNECT_MANUAL
                                putExtra("ip", ip)
                                putExtra("port", port.toIntOrNull() ?: 8080)
                            }
                            startService(intent)
                        },
                        onRestart = { startSyncService() }
                    )
                }
            }
        }
    }

    private fun startSyncService() {
        val intent = Intent(this, ClipboardSyncService::class.java)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            startForegroundService(intent)
        } else {
            startService(intent)
        }
    }
}

@Composable
fun MainScreen(
    status: String,
    error: String?,
    onManualConnect: (String, String) -> Unit,
    onRestart: () -> Unit
) {
    var ipAddress by remember { mutableStateOf("") }
    var port by remember { mutableStateOf("8080") }

    Column(modifier = Modifier.padding(16.dp)) {
        Text(
            text = "Clipboard Sync",
            style = MaterialTheme.typography.headlineMedium,
            modifier = Modifier.padding(bottom = 8.dp)
        )

        Card(
            modifier = Modifier.fillMaxWidth().padding(vertical = 8.dp),
            colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceVariant)
        ) {
            Column(modifier = Modifier.padding(16.dp)) {
                Text(text = "Status: $status", style = MaterialTheme.typography.titleMedium)
                if (error != null) {
                    Text(
                        text = "Error: $error",
                        color = MaterialTheme.colorScheme.error,
                        style = MaterialTheme.typography.bodySmall
                    )
                }
            }
        }

        Spacer(modifier = Modifier.height(16.dp))

        Text(text = "Manual Connection (Fallback)", style = MaterialTheme.typography.titleSmall)
        OutlinedTextField(
            value = ipAddress,
            onValueChange = { ipAddress = it },
            label = { Text("PC IP Address (e.g. 192.168.1.5)") },
            modifier = Modifier.fillMaxWidth()
        )
        OutlinedTextField(
            value = port,
            onValueChange = { port = it },
            label = { Text("Port") },
            modifier = Modifier.fillMaxWidth().padding(top = 8.dp)
        )

        Button(
            onClick = { onManualConnect(ipAddress, port) },
            modifier = Modifier.padding(top = 8.dp).fillMaxWidth()
        ) {
            Text("Connect Manually")
        }

        HorizontalDivider(modifier = Modifier.padding(vertical = 16.dp))

        Button(onClick = onRestart, modifier = Modifier.fillMaxWidth()) {
            Text("Restart Background Service")
        }

        Text(
            text = "Note: To send your clipboard to PC, use the 'Sync Local Clipboard' button in your notifications.",
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.secondary,
            modifier = Modifier.padding(top = 16.dp)
        )
    }
}


