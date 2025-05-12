Add-Type -AssemblyName Microsoft.VisualBasic

# Show popup to get IP address
$ip = [Microsoft.VisualBasic.Interaction]::InputBox("Enter the IP address to map to 'nifi':", "IP Input", "192.168.0.3")

# Check if input is empty
if (-not $ip -or $ip -match '^\s*$') {
    Write-Host "No IP provided. Exiting."
    exit 1
}

$hostsPath = "C:\Windows\System32\drivers\etc\hosts"

# Ensure script is running as Administrator
if (-not ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")) {
    Write-Host "Please run this script as Administrator."
    exit 1
}

# Remove existing 'nifi' lines and add the new one
$filteredLines = Get-Content $hostsPath | Where-Object { $_ -notmatch "\bnifi\b" }
$filteredLines += "$ip nifi"
$filteredLines | Set-Content $hostsPath

Write-Host "Updated hosts file successfully with: $ip nifi"
