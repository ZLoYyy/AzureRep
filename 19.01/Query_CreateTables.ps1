$dbName = "DE-Team-ShishporDenis-DB"
$user = "de-team-admin"
$password = "DEpassword0"
$server = "tcp:de-team.database.windows.net,1433"

$queryCreateTableProducts =
@"
CREATE TABLE [dbo].[Products](
[ID] [int] IDENTITY(1,1) PRIMARY KEY,
[Caption] [varchar](150) NOT NULL,
[Price] [decimal](18,2) NULL )
"@;
$queryCreateTableUsers = 
@"
CREATE TABLE [dbo].[Users](
[ID] [int] IDENTITY(1,1) PRIMARY KEY,
[LastName] [varchar](100) NOT NULL,
[FirstName] [varchar](100) NULL,
[SecondName] [varchar](100) NULL,
[Email] [varchar](255) NULL )
"@;
$sqlConn = New-Object System.Data.SqlClient.SqlConnection
$sqlConn.ConnectionString = ("Server={0};Initial Catalog={1};Persist Security Info=False;User ID={2};Password={3};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;" -f $server, $dbName, $user, $password)
try
{	#Open connection
	$sqlConn.Open()
	
	#Creacte command
	$sqlcmd = $sqlConn.CreateCommand()
	#Create table Products
	$sqlcmd.CommandText = $queryCreateTableProducts
	$sqlcmd.ExecuteNonQuery()

	#Create table Users
	$sqlcmd.CommandText = $queryCreateTableUsers
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

