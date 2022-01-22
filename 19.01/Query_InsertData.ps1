$dbName = "DE-Team-ShishporDenis-DB"
$user = "de-team-admin"
$password = "DEpassword0"
$server = "tcp:de-team.database.windows.net,1433"

$queryInsertDataInProducts =
@"
INSERT INTO [dbo].[Products]
           ([Caption], [Price])
     VALUES
           ('MainProduct', 250.3)
"@;
$queryInsertDataInUsers = 
@"
INSERT INTO [dbo].[Users]
           ([LastName], [FirstName], [SecondName], [Email])
     VALUES
           ('Ivanid', 'Ivan', 'Ivanovich', 'ivan@ivan.ru')
"@;
$sqlConn = New-Object System.Data.SqlClient.SqlConnection
$sqlConn.ConnectionString = ("Server={0};Initial Catalog={1};Persist Security Info=False;User ID={2};Password={3};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;" -f $server, $dbName, $user, $password)
try
{	#Open connection
	$sqlConn.Open()
	
	#Creacte command
	$sqlcmd = $sqlConn.CreateCommand()
	#Insert data in Products
	$sqlcmd.CommandText = $queryInsertDataInProducts
	$sqlcmd.ExecuteNonQuery()

	#Insert data in Users
	$sqlcmd.CommandText = $queryInsertDataInUsers
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