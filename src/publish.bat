@echo off
setlocal

set VERSION=%1
set APIKEY=%2

if "%VERSION%"=="" (
    echo Version is required as first parameter.
    exit /b 1
)

if "%APIKEY%"=="" (
    echo API key is required as second parameter.
    exit /b 1
)

set PACKAGES=^
PgOutput2Json.AzureEventHubs ^
PgOutput2Json.DynamoDb ^
PgOutput2Json.Kafka ^
PgOutput2Json.Kinesis ^
PgOutput2Json.MongoDb ^
PgOutput2Json.RabbitMq ^
PgOutput2Json.RabbitMqStreams ^
PgOutput2Json.Redis ^
PgOutput2Json.Sqlite ^
PgOutput2Json.Webhooks

for %%P in (%PACKAGES%) do (
    echo Pushing %%P version %VERSION%...
    dotnet nuget push .\%%P\bin\Release\%%P.%VERSION%.nupkg --source https://api.nuget.org/v3/index.json --api-key %APIKEY%
)

endlocal
