<#
    Сохранение данных в бд
#>
Function ToSql ($Data, $TableName) 
 {
    $dbName = "DBTest"
    $user = "DESKTOP-IFQ91DF\ZLoY"
    $password = ""
    $server = "DESKTOP-IFQ91DF\SQLEXPRESS01"

    $sqlConn = New-Object System.Data.SqlClient.SqlConnection
    $sqlConn.ConnectionString = ("Server={0};Initial Catalog={1};Persist Security Info=False;Integrated Security=true;User ID={2};Password={3};MultipleActiveResultSets=False;Encrypt=False;TrustServerCertificate=False;Connection Timeout=30;" -f $server, $dbName, $user, $password)
    try
    {
        #Open connection
	    $sqlConn.Open()

	    #Creacte command
	    $sqlcmd = $sqlConn.CreateCommand()

    Foreach ($subData in $Data)
    {
        $sqlcmd.Parameters.Clear()
        switch ( $TableName )
        {
            "SMS_ore_supply_0_300" 
            { 
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
                $sqlcmd.Parameters["@date"].Value = ("{0}" -f $subData.date) 
                $sqlcmd.Parameters["@work_shift"].Value = ("{0}" -f $subData.work_shift) 
                $sqlcmd.Parameters["@charge_number"].Value = ("{0}" -f $subData.charge_number) 
                $sqlcmd.Parameters["@fe_percent"].Value = ("{0}" -f $subData.fe_percent) 
                $sqlcmd.Parameters["@ton_count"].Value = ("{0}" -f $subData.ton_count) 
                $sqlcmd.Parameters["@metal_ton_count"].Value = ("{0}" -f $subData.metal_ton_count) 
            }
            "SMS_ore_output_0_10" 
            { 
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
                $sqlcmd.Parameters["@date"].Value = ("{0}" -f $subData.date)
                $sqlcmd.Parameters["@type"].Value = ("{0}" -f $subData.type)
                $sqlcmd.Parameters["@charge_number"].Value = ("{0}" -f $subData.charge_number)
                $sqlcmd.Parameters["@fe_percent"].Value = ("{0}" -f $subData.fe_percent)
                $sqlcmd.Parameters["@ton_count"].Value =("{0}" -f $subData.ton_count)
                $sqlcmd.Parameters["@metal_ton_count"].Value = ("{0}" -f $subData.metal_ton_count)
            }
            "SMS_total_ore_supply" 
            {
             $sqlcmd.CommandText =
@"
IF NOT EXISTS (select 1 from SMS_total_ore_supply where month=@month)
BEGIN
INSERT INTO [SMS_total_ore_supply] ([month],[value])
                          values  (@month,@value)
END
"@;  
                
                $sqlcmd.Parameters.Add("@month", [Data.SQLDBType]::NVarChar, 50)
                $sqlcmd.Parameters.Add("@value", [Data.SQLDBType]::NVarChar, 50)
                $sqlcmd.Parameters["@month"].Value = ("{0}" -f $subData.month)
                $sqlcmd.Parameters["@value"].Value = ("{0}" -f $subData.value)
            }
            "SMS_salable_ore_stacks_0_10" 
            { 
            $sqlcmd.CommandText =
@"
IF NOT EXISTS (select 1 from SMS_salable_ore_stacks_0_10 where stack_number=@stack_number AND month=@month)
BEGIN
INSERT INTO [SMS_salable_ore_stacks_0_10] ([stack_number],[actual_tonn_count],[fe_percent],[metal_ton_count],[notes],[month])
                                  values  (@stack_number,@actual_tonn_count,@fe_percent,@metal_ton_count,@notes,@month )
END
"@; 
                
                $sqlcmd.Parameters.Add("@stack_number", [Data.SQLDBType]::NVarChar, 50)
                $sqlcmd.Parameters.Add("@actual_tonn_count", [Data.SQLDBType]::NVarChar, 50)
                $sqlcmd.Parameters.Add("@fe_percent", [Data.SQLDBType]::NVarChar, 50)
                $sqlcmd.Parameters.Add("@metal_ton_count", [Data.SQLDBType]::NVarChar, 50)
                $sqlcmd.Parameters.Add("@notes", [Data.SQLDBType]::NVarChar, 50)
                $sqlcmd.Parameters.Add("@month", [Data.SQLDBType]::NVarChar, 50)
                $sqlcmd.Parameters["@stack_number"].Value = ("{0}" -f $subData.stack_number)
                $sqlcmd.Parameters["@actual_tonn_count"].Value = ("{0}" -f $subData.actual_tonn_count)
                $sqlcmd.Parameters["@fe_percent"].Value = ("{0}" -f $subData.fe_percent)
                $sqlcmd.Parameters["@metal_ton_count"].Value = ("{0}" -f $subData.metal_ton_count)
                $sqlcmd.Parameters["@notes"].Value = ("{0}" -f $subData.notes)
                $sqlcmd.Parameters["@month"].Value = ("{0}" -f $subData.month)
            }
            "SMS_Tailings_1_dump" 
            { 
            $sqlcmd.CommandText =
@"
IF NOT EXISTS (select 1 from SMS_Tailings_1_dump where dump_number=@dump_number AND month=@month)
BEGIN
INSERT INTO [SMS_Tailings_1_dump] ([dump_number],[ton_count],[metal_count],[percent],[month])
                          values  (@dump_number,@ton_count,@metal_count,@percent,@month)
END
"@; 

                $sqlcmd.Parameters.Add("@dump_number", [Data.SQLDBType]::NVarChar, 50)
                $sqlcmd.Parameters.Add("@ton_count", [Data.SQLDBType]::NVarChar, 50)
                $sqlcmd.Parameters.Add("@metal_count", [Data.SQLDBType]::NVarChar, 50)
                $sqlcmd.Parameters.Add("@percent", [Data.SQLDBType]::NVarChar, 50)
                $sqlcmd.Parameters.Add("@month", [Data.SQLDBType]::NVarChar, 50)
                $sqlcmd.Parameters["@dump_number"].Value = ("{0}" -f $subData.dump_number)
                $sqlcmd.Parameters["@ton_count"].Value = ("{0}" -f $subData.ton_count)
                $sqlcmd.Parameters["@metal_count"].Value = ("{0}" -f $subData.metal_count)
                $sqlcmd.Parameters["@percent"].Value = ("{0}" -f $subData.percent)
                $sqlcmd.Parameters["@month"].Value = ("{0}" -f $subData.month)
            }
        }
	    $sqlcmd.ExecuteNonQuery()

}
	    #Close connection	
	    $sqlConn.Close()
    }
    catch
    {	
	    $sqlConn.Close()
	    Write-Error -Message $_.Exception
            throw $_.Exception
    }
 }

$script:SMS_ore_supply_0_300_data = @()
$script:SMS_ore_output_0_10_data = @()
$script:SMS_total_ore_supply_data = @()
$script:SMS_salable_ore_stacks_0_10_data = @()
$script:SMS_Tailings_1_dump_data = @()
<#
    Получение данных из xlsx
#>
Function ReadData 
{
    param(
[string]$Path,
[string]$Sheet
)

    #Подача руды 0-300
    $SMS_ore_supply_0_300 = Import-Excel -Path $Path -StartRow 5 -StartColumn 2 -EndColumn 7 -WorksheetName $Sheet
    #Выход руды 0-10
    $SMS_ore_output_0_10 = Import-Excel -Path $Path -StartRow 5 -StartColumn 8 -EndColumn 15 -WorksheetName $Sheet
    #ИТОГО ПОДАЧА РУДЫ в месяц
    $SMS_total_ore_supply = Import-Excel -Path $Path -StartRow 1 -StartColumn 16 -EndColumn 16 -WorksheetName $Sheet
    #ШТАБЕЛИ ТОВАРНОЙ РУДЫ 0-10
    $SMS_salable_ore_stacks_0_10 = Import-Excel -Path $Path -StartRow 2 -StartColumn 17 -EndColumn 22 -WorksheetName $Sheet
    #Хвосты-1  Отвал
    $SMS_Tailings_1_dump = Import-Excel -Path $Path -StartRow 2 -StartColumn 23 -EndColumn 26 -WorksheetName $Sheet



    ForEach ($ExcelLok in $SMS_ore_supply_0_300)
    {
        if($ExcelLok.Дата -eq $null){continue}
        try{
            $RowHash = @{ 
                      date = [DateTime]::FromOADate($ExcelLok.Дата)
                      work_shift = $ExcelLok.Смена
                      charge_number = $ExcelLok.'№ шихты'
                      fe_percent = $ExcelLok.'%Fe'
                      ton_count = $ExcelLok.тонн
                      metal_ton_count = $ExcelLok.'металл, т'
                    }
            $script:SMS_ore_supply_0_300_data += $RowHash
        }
        catch{continue}    
    }
    $date = ""
    ForEach ($ExcelLok in $SMS_ore_output_0_10)
    {
        if($ExcelLok.Дата -eq $null -and $ExcelLok.'№ шихты' -eq $null -and $ExcelLok.тип -eq $null){continue}
        if($ExcelLok.Дата -eq $null){$ExcelLok.Дата = $date}
        $date = $ExcelLok.Дата
        try
        {
            $RowHash = @{ 
                          date = [DateTime]::FromOADate($ExcelLok.Дата)
                          charge_number = $ExcelLok.'№ шихты'
                          type = $ExcelLok.тип
                          fe_percent = $ExcelLok.'%Fe'
                          ton_count = $ExcelLok.тонн
                          metal_ton_count = $ExcelLok.'металл, т'
                        }

                $script:SMS_ore_output_0_10_data += $RowHash
        }
        catch{continue}
    
    }

    ForEach ($ExcelLok in $SMS_total_ore_supply)
    {
        $ExcelLok
        if($ExcelLok.'ИТОГО ПОДАЧА РУДЫ в месяц' -eq $null){continue}
 
        $RowHash = @{ 
                      month = $Sheet
                      value = $ExcelLok.'ИТОГО ПОДАЧА РУДЫ в месяц'
                    }
        
         $script:SMS_total_ore_supply_data += $RowHash
    }

    ForEach ($ExcelLok in $SMS_salable_ore_stacks_0_10)
    {
        if($ExcelLok.'№ п/п' -eq $null){continue}

        $RowHash = @{ stack_number = $ExcelLok.'№ штабеля' 
                      actual_tonn_count = $ExcelLok.'Фактич. остаток т.'
                      fe_percent = $ExcelLok.'Fe%'
                      metal_ton_count = $ExcelLok.'Металл в т.'
                      notes = $ExcelLok.Примечания
                      month = $Sheet
                    }
     
        $script:SMS_salable_ore_stacks_0_10_data += $RowHash
    }

    ForEach ($ExcelLok in $SMS_Tailings_1_dump)
    {
        if($ExcelLok.'№ отвала' -eq $null){continue}
    
        $RowHash = @{ 
                      dump_number = $ExcelLok.'№ отвала' 
                      ton_count = $ExcelLok.тонн
                      metal_count = $ExcelLok.металл
                      percent = $ExcelLok.'%'
                      month = $Sheet
                    }

        $script:SMS_Tailings_1_dump_data += $RowHash
    }
}

$WorkSheets = Get-ExcelSheetInfo -Path E:\Shedule\Test2Сентябрь.csv

#Перебираем все листы
Foreach($sheet in $WorkSheets)
{
    ReadData -Path E:\Shedule\Test2.xlsx -Sheet $sheet.Name
    ToSql -Data $script:SMS_ore_supply_0_300_data -TableName "SMS_ore_supply_0_300"
    ToSql -Data $script:SMS_ore_output_0_10_data -TableName "SMS_ore_output_0_10"
    ToSql -Data $script:SMS_total_ore_supply_data -TableName "SMS_total_ore_supply"
    ToSql -Data $script:SMS_salable_ore_stacks_0_10_data -TableName "SMS_salable_ore_stacks_0_10"
    ToSql -Data $script:SMS_Tailings_1_dump_data -TableName "SMS_Tailings_1_dump"
}