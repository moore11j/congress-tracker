Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing
$ErrorActionPreference = 'Stop'
$envPath = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\.env.local'))
$form = New-Object Windows.Forms.Form
$form.Text = 'Walnut - Alpaca API setup'
$form.Size = New-Object Drawing.Size(540, 315)
$form.StartPosition = 'CenterScreen'
$form.TopMost = $true
$form.FormBorderStyle = 'FixedDialog'
$form.MaximizeBox = $false
$info = New-Object Windows.Forms.Label
$info.Text = 'Enter your Alpaca Paper Trading API keys. Both fields are masked. Keys are saved only in backend/.env.local on this computer.'
$info.SetBounds(20, 15, 485, 50)
$form.Controls.Add($info)
$keyLabel = New-Object Windows.Forms.Label
$keyLabel.Text = 'Key (API key ID)'
$keyLabel.SetBounds(20, 75, 120, 24)
$form.Controls.Add($keyLabel)
$keyInput = New-Object Windows.Forms.TextBox
$keyInput.UseSystemPasswordChar = $true
$keyInput.SetBounds(150, 72, 350, 26)
$form.Controls.Add($keyInput)
$secretLabel = New-Object Windows.Forms.Label
$secretLabel.Text = 'Secret key'
$secretLabel.SetBounds(20, 120, 120, 24)
$form.Controls.Add($secretLabel)
$secretInput = New-Object Windows.Forms.TextBox
$secretInput.UseSystemPasswordChar = $true
$secretInput.SetBounds(150, 117, 350, 26)
$form.Controls.Add($secretInput)
$statusLabel = New-Object Windows.Forms.Label
$statusLabel.SetBounds(20, 165, 480, 40)
$form.Controls.Add($statusLabel)
$save = New-Object Windows.Forms.Button
$save.Text = 'Save locally'
$save.SetBounds(365, 218, 135, 32)
$save.Add_Click({
    if ($keyInput.Text.Trim() -notmatch '^[A-Za-z0-9_-]{10,200}$' -or $secretInput.Text.Trim() -notmatch '^[A-Za-z0-9_-]{10,200}$') {
        $statusLabel.Text = 'Enter both keys exactly as provided by Alpaca.'
        return
    }
    try {
        $lines = if (Test-Path -LiteralPath $envPath) { [IO.File]::ReadAllLines($envPath) } else { @() }
        $kept = @($lines | Where-Object { $_ -notmatch '^\s*(export\s+)?(APCA_API_KEY_ID|APCA_API_SECRET_KEY)\s*=' })
        $kept += 'APCA_API_KEY_ID=' + $keyInput.Text.Trim()
        $kept += 'APCA_API_SECRET_KEY=' + $secretInput.Text.Trim()
        [IO.File]::WriteAllLines($envPath, $kept, (New-Object Text.UTF8Encoding($false)))
        $keyInput.Clear()
        $secretInput.Clear()
        $form.DialogResult = [Windows.Forms.DialogResult]::OK
        $form.Close()
    } catch {
        $statusLabel.Text = 'Unable to save the local configuration. No credentials were logged.'
    }
})
$form.Controls.Add($save)
$form.AcceptButton = $save
if ($form.ShowDialog() -eq [Windows.Forms.DialogResult]::OK) { Write-Output 'Alpaca credentials saved locally.' }
else { Write-Output 'Alpaca setup cancelled.' }
$form.Dispose()
