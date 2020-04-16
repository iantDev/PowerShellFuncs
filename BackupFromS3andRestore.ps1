#
# BackupFromS3andRestore.ps1
#
Import-Module "C:\Program Files (x86)\AWS Tools\PowerShell\AWSPowerShell\AWSPowerShell.psd1"
Import-Module $PSScriptRoot\DebugFunctions.ps1 -Force
Import-Module $PSScriptRoot\ServerConnConfig.ps1 -Force
Import-Module $PSScriptRoot\Config.ps1 -Force

[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Smo") | Out-Null
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.SmoExtended") | Out-Null


$BucketName = "autoalert-dbbackup"
$datePrefix = Get-Date -Format "yyyy_MM_dd"
$dayNum = Get-Date -Format "dd"
$localPath = "N:\FTP\"

## $dbList from config

$dbDataName = ""
$dbLogName = ""
$dbDataFileName = ""
$dbLogFileName = ""
$dbDataPath = ""
$dbLogPath = ""

#destination server
[String] $dstServer = $serverConnConfig.stage
Set-AWSCredentials -AccessKey $AWSCred.accessKey -SecretKey $AWSCred.secret -StoreAs AABackUpCredentials
Initialize-AWSDefaults -ProfileName AABackUpCredentials  -Region us-east-1

$connDst =New-Object System.Data.SqlClient.SqlConnection ("Persist Security Info=False;Integrated Security=true;server={0}" -f $dstServer)
$SmoDstSrv = New-Object Microsoft.SqlServer.Management.Smo.Server ($connDst)
# $SmoDstSrv.ConnectionContext.ConnectTimeout = 0
$SmoDstSrv.ConnectionContext.StatementTimeout = 0

$connDst.ConnectionTimeout = 0


foreach ($db in $dbList){
	# $key = "daily/IS/IntegrationService_backup_2016_11"
	if ($dayNum -eq "01") {
		$prefix = "monthly" + "/dbprod2/$db" + "_backup_"
	}
	else {
		$prefix = "daily" + "/dbprod2/$db" + "_backup_"
	}
	write-output "prefix=" $prefix 

	$s3Obj = Get-S3Object -BucketName $BucketName -KeyPrefix $prefix | Sort-Object -Property LastModified | Select-Object -last 1
		write-output $s3Obj.key 
	$fileName = $s3Obj.key -replace $prefix , ($db + "_backup_")
	$localFilePath = [IO.Path]::Combine($localPath, $fileName)
	write-output "localFilePath="$localFilePath 
	if (!(Test-Path $localFilePath)) {
#		Copy-S3Object -BucketName $BucketName -Key $s3Obj.Key -LocalFile $localFilePath
		Write-Output "Download process $localFilePath is done." 
	} 
	else {Write-Output "$localFilePath already exists."}

	$dbObj = $SmoDstSrv.Databases[$db]
		

	$restore = New-Object Microsoft.SqlServer.Management.Smo.Restore
	$backupFile = New-Object Microsoft.SqlServer.Management.Smo.BackupDeviceItem($localFilePath,"File")
	$restore.Devices.Add($backupFile)
	$BakcupInfo = $restore.ReadMediaHeader($SmoDstSrv)
	$BackupFileInfo = $restore.ReadFileList($SmoDstSrv)
	
	$MediaBackpDate = $BakcupInfo.Rows[0]['MediaDate']
	$sql = "SELECT *
			FROM (
				SELECT
				num = RANK() OVER (PARTITION BY rs.destination_database_name ORDER BY rs.restore_date DESC, bs.backup_start_date desc),
				[rs].[destination_database_name], 
				[rs].[restore_date], 
				[bs].[backup_start_date], 
				[bs].[backup_finish_date], 
				bs.database_name AS srcDbName,
				[bmf].[physical_device_name] as [backup_file_used_for_restore]
				FROM msdb..restorehistory rs
				INNER JOIN msdb..backupset bs ON [rs].[backup_set_id] = [bs].[backup_set_id]
				INNER JOIN msdb..backupmediafamily bmf  ON [bs].[media_set_id] = [bmf].[media_set_id] 				
				WHERE rs.destination_database_name IN('{0}', '{1}')				
			) d
			WHERE d. num = 1
			ORDER BY d.backup_start_date
			" -f $db, ($db + "_1")

	$lastRestoreMeta = $SmoDstSrv.ConnectionContext.ExecuteWithResults($sql)
	if ($null -ne $lastRestoreMeta.Tables[0].Rows[1]["backup_start_date"]){
		$lastRestoreBackupDate = $lastRestoreMeta.Tables[0].Rows[1]["backup_start_date"]
	} else {
		$lastRestoreBackupDate = $null
	}
	
	# if no existing _1 db, use dbName. else use _1
	# if the _1 is the lastest restore, use db for the dbName. else use _1
	
	if ($null -eq $lastRestoreMeta.Tables[0].Rows[1] -or $lastRestoreMeta.Tables[0].Rows[1]["destination_database_name"] -like "*_1")) {			
		$dbName = $db
		$dbDataPath = "D:\DATA"
		$dbLogPath = "K:\AALog"		
		$dbDataPath = [IO.Path]::Combine($dbDataPath, ($BackupFileInfo.Rows[0]['PhysicalName'] | Split-Path -Leaf))		
		$dbLogPath = [IO.Path]::Combine($dbLogPath, ($BackupFileInfo.Rows[1]['PhysicalName'] | Split-Path -Leaf))		
	}
	else {
		$dbName = $db + "_1"
		$dbDataPath = "M:\DATA"
		$dbLogPath = "M:\LOG"
		$dbDataFileName = ($BackupFileInfo.Rows[0]['PhysicalName'] | Split-Path -Leaf).Replace($db, $dbName)
		$dbDataPath = [IO.Path]::Combine($dbDataPath, $dbDataFileName)
		$dbLogFileName = ($BackupFileInfo.Rows[1]['PhysicalName'] | Split-Path -Leaf).Replace($db, $dbName)
		$dbLogPath = [IO.Path]::Combine($dbLogPath, $dbLogFileName)	
	}
	$dbDataName = $BackupFileInfo.Rows[0]['LogicalName']
	$dbLogName = $BackupFileInfo.Rows[1]['LogicalName']
	
	Write-Output "MediaBackpDate = $MediaBackpDate `r`n lastRestoreBackupDate = $lastRestoreBackupDate"

	
	#replace older db
	<#
	if ($dbModDate -lt $db_1BModDate ) {
		$dbName = $db
		$dbDataPath = "D:\DATA"
		$dbLogPath = "K:\AALog"
		$latestDbModDate = $db_1BModDate
		$fileName = $dbObj.FileGroups['primary'].Files[0].FileName | Split-Path -Leaf
		$dbDataPath = [IO.Path]::Combine($dbDataPath, $fileName)
		$fileName = $dbObj.LogFiles[0].FileName | Split-Path -Leaf
		$dbLogPath = [IO.Path]::Combine($dbLogPath, $fileName)
		
	} 
	else {
		$dbName = $db + "_1"
		$dbDataPath = "M:\DATA"
		$dbLogPath = "M:\LOG"
		$latestDbModDate = $dbModDate
		$fileName = $dbObj.FileGroups['primary'].Files[0].FileName | Split-Path -Leaf
		$fileName = $fileName.Replace(".mdf", "_1.mdf")
		$dbDataPath = [IO.Path]::Combine($dbDataPath, $fileName)
		$fileName = $dbObj.LogFiles[0].FileName | Split-Path -Leaf
		$fileName = $fileName.Replace(".ldf", "_1.ldf")
		$dbLogPath = [IO.Path]::Combine($dbLogPath, $fileName)
	}	
	#>
	if ($MediaBackpDate -gt $lastRestoreBackupDate){
		$sql = New-Object System.Collections.ArrayList
		# $sql.Add("ALTER DATABASE [$dbName] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
		$sql.Add("
				ALTER DATABASE [$dbName] SET SINGLE_USER WITH ROLLBACK IMMEDIATE		
				Restore Database $dbName from Disk = '$localFilePath'
				With Recovery, Replace, stats = 10,
				move '$dbDataName' to '$dbDataPath',
				move '$dbLogName' to '$dbLogPath'
			")
		try {
		$SmoDstSrv.ConnectionContext.ExecuteNonQuery($sql)
		#$sql
		}
		catch {
			Write-Exception -obj $db -userMsg "Error occurred when restoring $dbName from $localFilePath. " -appendMsg $true
		}
	}
	else {Write-Output "$db series had the latest data from backup already."}
	Write-Output "$db is processed. `r`n"
}

Write-Host "The process is done." 
