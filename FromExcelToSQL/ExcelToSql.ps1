Function ToDB()
{
    param(
        $ArrayRow, 
        [int]$TableID,
        [DateTime]$Date
    )

    $dbName = "ExportedFromExcel"
    $user = "de-team-admin"
    $password = "DEpassword0"
    $server = "tcp:de-team.database.windows.net,1433"
    
    $sqlConn = New-Object System.Data.SqlClient.SqlConnection
    $sqlConn.ConnectionString = ("Server={0};Initial Catalog={1};Persist Security Info=False;User ID={2};Password={3};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;" -f $server, $dbName, $user, $password)

    try
    {
        #Open connection
	    $sqlConn.Open()

	    #Creacte command
	    $sqlcmd = $sqlConn.CreateCommand()
        switch ( $TableID )
        {    
            1 
            {
                try
                {
                    $date = ("{0}" -f [DateTime]::FromOADate($ArrayRow.'Дата')) 
                }
                catch
                {
                    if($script:SMS_ore_supply_0_300_date)
                    {
                        $date = $script:SMS_ore_supply_0_300_date
                    }
                }
                $script:SMS_ore_supply_0_300_date = $date
                $work_shift = ("{0}" -f $ArrayRow.'Смена/отбор пробы') 
                $charge_number = ("{0}" -f $ArrayRow.'№ шихты') 
                $fe_percent = ("{0}" -f $ArrayRow.'%Fe') 
                $ton_count = ("{0}" -f $ArrayRow.'тонн') 
                $metal_ton_count = ("{0}" -f $ArrayRow.'металл, т')
                if(!$work_shift -and !$charge_number -and !$fe_percent -and !$ton_count -and !$metal_ton_count)  
                { 
                    $sqlConn.Close()
                    return 
                }
                
                $sqlcmd.CommandText =
@"
    IF NOT EXISTS (select 1 from SMS_ore_supply_0_300 where date=@date AND work_shift=@work_shift)
    BEGIN
        INSERT INTO [SMS_ore_supply_0_300] ([date],[work_shift],[charge_number],[fe_percent],[ton_count],[metal_ton_count] )
                              values  (@date,@work_shift,@charge_number,@fe_percent,@ton_count,@metal_ton_count )
    END

"@; 

                    $sqlcmd.Parameters.Add("@date", [Data.SQLDBType]::NVarChar, 50)
                    $sqlcmd.Parameters.Add("@work_shift", [Data.SQLDBType]::NVarChar, 50)
                    $sqlcmd.Parameters.Add("@charge_number", [Data.SQLDBType]::NVarChar, 50)
                    $sqlcmd.Parameters.Add("@fe_percent", [Data.SQLDBType]::NVarChar, 50)
                    $sqlcmd.Parameters.Add("@ton_count", [Data.SQLDBType]::NVarChar, 50)
                    $sqlcmd.Parameters.Add("@metal_ton_count", [Data.SQLDBType]::NVarChar, 50)
                    $sqlcmd.Parameters["@date"].Value = $date
                    $sqlcmd.Parameters["@work_shift"].Value = $work_shift
                    $sqlcmd.Parameters["@charge_number"].Value = $charge_number
                    $sqlcmd.Parameters["@fe_percent"].Value = $fe_percent
                    $sqlcmd.Parameters["@ton_count"].Value = $ton_count
                    $sqlcmd.Parameters["@metal_ton_count"].Value = $metal_ton_count
            }
            2 
            { 
                try
                {
                    $date = ("{0}" -f [DateTime]::FromOADate($ArrayRow.'Дата')) 
                }
                catch
                {
                    if($script:SMS_ore_output_0_10_date)
                    {
                        $date = $script:SMS_ore_output_0_10_date
                    }
                }
                $script:SMS_ore_output_0_10_date = $date
                $type = ("{0}" -f $ArrayRow.'тип')
                $charge_number = ("{0}" -f $ArrayRow.'№ шихты') 
                $fe_percent = ("{0}" -f $ArrayRow.'%Fe') 
                $ton_count = ("{0}" -f $ArrayRow.'тонн') 
                $metal_ton_count = ("{0}" -f $ArrayRow.'металл, т')
                if(!$type -and !$charge_number -and !$fe_percent -and !$ton_count -and !$metal_ton_count)  
                { 
                    $sqlConn.Close()
                    return 
                }
                $sqlcmd.CommandText =
@"
    IF NOT EXISTS (select 1 from SMS_ore_output_0_10 where date=@date AND type=@type)
    BEGIN
    INSERT INTO [SMS_ore_output_0_10] ([date],[type],[charge_number],[fe_percent],[ton_count],[metal_ton_count] )
                              values  (@date,@type,@charge_number,@fe_percent,@ton_count,@metal_ton_count )
    END
"@;    

                    $sqlcmd.Parameters.Add("@date", [Data.SQLDBType]::NVarChar, 50)
                    $sqlcmd.Parameters.Add("@type", [Data.SQLDBType]::NVarChar, 50)
                    $sqlcmd.Parameters.Add("@charge_number", [Data.SQLDBType]::NVarChar, 50)
                    $sqlcmd.Parameters.Add("@fe_percent", [Data.SQLDBType]::NVarChar, 50)
                    $sqlcmd.Parameters.Add("@ton_count", [Data.SQLDBType]::NVarChar, 50)
                    $sqlcmd.Parameters.Add("@metal_ton_count", [Data.SQLDBType]::NVarChar, 50)
                    $sqlcmd.Parameters["@date"].Value = $date
                    $sqlcmd.Parameters["@type"].Value = $type
                    $sqlcmd.Parameters["@charge_number"].Value = $charge_number
                    $sqlcmd.Parameters["@fe_percent"].Value = $fe_percent
                    $sqlcmd.Parameters["@ton_count"].Value = $ton_count
                    $sqlcmd.Parameters["@metal_ton_count"].Value = $metal_ton_count
            }
            3 
            {
                $value = ("{0}" -f $ArrayRow.'ИТОГО ПОДАЧА РУДЫ в месяц') 
                
                if(!$value)  
                { 
                    $sqlConn.Close()
                    return 
                }

                $sqlcmd.CommandText =
@"
    IF NOT EXISTS (select 1 from SMS_total_ore_supply WHERE YEAR(date)=YEAR(@date) AND MONTH(date)=MONTH(@date))
    BEGIN
    INSERT INTO [SMS_total_ore_supply] ([date],[value])
                              values  (@date,@value)
    END
"@;  
                
                    $sqlcmd.Parameters.Add("@date", [Data.SQLDBType]::DateTime, 30)
                    $sqlcmd.Parameters.Add("@value", [Data.SQLDBType]::NVarChar, 50)
                    $sqlcmd.Parameters["@date"].Value = $Date
                    $sqlcmd.Parameters["@value"].Value = $value
            }
            4 
            {

                $stack_number = ("{0}" -f $ArrayRow.'№ штабеля')
                $actual_tonn_count = ("{0}" -f $ArrayRow.'Фактич. остаток т.')
                $fe_percent = ("{0}" -f $ArrayRow.'Fe%')
                $metal_ton_count = ("{0}" -f $ArrayRow.'Металл в т.')
                $notes = ("{0}" -f $ArrayRow.'Примечания')
                               
                if(!$stack_number -and !$actual_tonn_count -and !$fe_percent -and !$metal_ton_count)  
                { 
                    $sqlConn.Close()
                    return 
                }

                $sqlcmd.CommandText =
@"
    IF NOT EXISTS (select 1 from SMS_salable_ore_stacks_0_10 where stack_number=@stack_number AND YEAR(date)=YEAR(@date) AND MONTH(date)=MONTH(@date))
    BEGIN
    INSERT INTO [SMS_salable_ore_stacks_0_10] ([stack_number],[actual_tonn_count],[fe_percent],[metal_ton_count],[notes],[date])
                                      values  (@stack_number,@actual_tonn_count,@fe_percent,@metal_ton_count,@notes,@date)
    END
"@; 
                
                    $sqlcmd.Parameters.Add("@stack_number", [Data.SQLDBType]::NVarChar, 50)
                    $sqlcmd.Parameters.Add("@actual_tonn_count", [Data.SQLDBType]::NVarChar, 50)
                    $sqlcmd.Parameters.Add("@fe_percent", [Data.SQLDBType]::NVarChar, 50)
                    $sqlcmd.Parameters.Add("@metal_ton_count", [Data.SQLDBType]::NVarChar, 50)
                    $sqlcmd.Parameters.Add("@notes", [Data.SQLDBType]::NVarChar, 50)
                    $sqlcmd.Parameters.Add("@date", [Data.SQLDBType]::DateTime, 30)
                    $sqlcmd.Parameters["@stack_number"].Value = $stack_number
                    $sqlcmd.Parameters["@actual_tonn_count"].Value = $actual_tonn_count
                    $sqlcmd.Parameters["@fe_percent"].Value = $fe_percent
                    $sqlcmd.Parameters["@metal_ton_count"].Value = $metal_ton_count
                    $sqlcmd.Parameters["@notes"].Value = $notes
                    $sqlcmd.Parameters["@date"].Value = $Date
            }
            5 
            {
                $dump_number = ("{0}" -f $ArrayRow.'№ отвала')
                $ton_count = ("{0}" -f $ArrayRow.'тонн')
                $metal_count = ("{0}" -f $ArrayRow.'металл')
                $percent = ("{0}" -f $ArrayRow.'%')
                               
                if(!$ton_count -and !$metal_count -and !$percent)  
                { 
                    $sqlConn.Close()
                    return 
                }

                $sqlcmd.CommandText =
@"
    IF NOT EXISTS (select 1 from SMS_Tailings_1_dump where dump_number=@dump_number AND YEAR(date)=YEAR(@date) AND MONTH(date)=MONTH(@date))
    BEGIN
    INSERT INTO [SMS_Tailings_1_dump] ([dump_number],[ton_count],[metal_count],[percent],[date])
                              values  (@dump_number,@ton_count,@metal_count,@percent,@date)
    END
"@; 

                    $sqlcmd.Parameters.Add("@dump_number", [Data.SQLDBType]::NVarChar, 50)
                    $sqlcmd.Parameters.Add("@ton_count", [Data.SQLDBType]::NVarChar, 50)
                    $sqlcmd.Parameters.Add("@metal_count", [Data.SQLDBType]::NVarChar, 50)
                    $sqlcmd.Parameters.Add("@percent", [Data.SQLDBType]::NVarChar, 50)
                    $sqlcmd.Parameters.Add("@date", [Data.SQLDBType]::DateTime, 30)
                    $sqlcmd.Parameters["@dump_number"].Value = $dump_number
                    $sqlcmd.Parameters["@ton_count"].Value = $ton_count
                    $sqlcmd.Parameters["@metal_count"].Value = $metal_count
                    $sqlcmd.Parameters["@percent"].Value = $percent
                    $sqlcmd.Parameters["@date"].Value = $Date
            }
        }
    
        $sqlcmd.ExecuteNonQuery()

        #Close connection	
	    $sqlConn.Close()
    }
    catch
    {
        $sqlConn.Close()
	    Write-Error -Message $_.Exception
            throw $_.Exception
    }
    
    #$text = "ID="+$TableID
    #Write-Output $text
    #$ArrayRow
}

$RunAsConnection = Get-AutomationConnection -Name "AzureRunAsConnection"
Connect-AzAccount -ServicePrincipal -Tenant $RunAsConnection.TenantId -ApplicationId $RunAsConnection.ApplicationId -CertificateThumbprint $RunAsConnection.CertificateThumbprint | Write-Verbose
Set-AzContext -Subscription $RunAsConnection.SubscriptionID | Write-Verbose

$resourceGroupName="azureapp-auto-alerts-6b09d6-d_shishpor_aspex_kz"  
$StorageAccountName="deshishpor"  
$containerName="xlsx-file" 

$storageAcc=Get-AzStorageAccount -ResourceGroupName $resourceGroupName -Name $StorageAccountName 
$Context=$storageAcc.Context  

$blobs=Get-AzStorageBlob -Container $containerName -Context $Context

foreach($blob in $blobs)
{
    $encoding = New-Object System.Text.UTF8Encoding
	$delimiter = ","
	$reader = $blob.OpenRead() 
	$reader = New-Object System.IO.StreamReader($reader)
	$headers = $reader.ReadLine() -split $delimiter 
	$result = [ordered]@{}    
    
	foreach($header in $headers) 
	{        
		$result[$header] = 0
	}
    
    $name = $blob.Name.Split(".")
    
    # Формат файла [Содержание файла]_[год]_[Название листа]_[ид таблицы].csv 
    $FileData = $name.Split('_')[0]
    $Year = $name.Split('_')[1]
    $SheetName = $name.Split('_')[2]
    $id = $name.Split('_')[3]
    $Month = 0

    switch ( $SheetName )
    {    
            "Январь" 
            { $Month = 1 }
            "Февраль" 
            { $Month = 2 }
            "Март" 
            { $Month = 3 }
            "Апрель" 
            { $Month = 4 }
            "Май" 
            { $Month = 5 }
            "Июнь" 
            { $Month = 6 }
            "Июль" 
            { $Month = 7 }
            "Август" 
            { $Month = 8 }
            "Сентябрь" 
            { $Month = 9 }
            "Октябрь" 
            { $Month = 10 }
            "Ноябрь" 
            { $Month = 11 }
            "Декабрь" 
            { $Month = 12 }
    }

    $str = $Year +'-'+ $Month +"-01" 
    $Date =  [DateTime]$str

    while(-not $reader.EndOfStream) 
	{
		# Get Header  in first line
		try{		
           
		    $csv = $reader.ReadLine() | ConvertFrom-Csv -Header $headers -Delimiter $delimiter 
            
            #Удаляем ненужные свойства
            $csv.PSObject.Properties.Remove('ItemInternalId')
            $csv.PSObject.Properties.Remove('@odata.etag')
            
            ToDB -ArrayRow  $csv -TableID $id -Date $Date
		}
	    catch
        {
            Write-Error -Message $_.Exception
            throw $_.Exception
        }
	}
}
