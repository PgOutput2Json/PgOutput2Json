@echo off
setlocal EnableDelayedExpansion

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

rem Core first: every adapter declares a dependency on it, so if a push fails part
rem way through it is better to have core published and some adapters missing
rem than the reverse.
set PACKAGES=^
PgOutput2Json ^
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

rem ---------------------------------------------------------------------------
rem Pre-flight: check that every package was built at %VERSION% before pushing
rem anything.
rem
rem A project-specific <Version> left in a csproj (an unpushed hotfix) makes that
rem package build at its own version, so there is no %VERSION% nupkg for it.
rem Catching it here matters because the push loop below cannot be re-run after a
rem partial failure - NuGet rejects a version that already exists, so a
rem half-published set has to be finished by hand.
rem ---------------------------------------------------------------------------
set MISSING=
set FOUND=0

for %%P in (%PACKAGES%) do (
    if exist ".\%%P\bin\Release\%%P.%VERSION%.nupkg" (
        set /a FOUND+=1
    ) else (
        set MISSING=!MISSING! %%P
    )
)

if not "!MISSING!"=="" (
    echo.
    echo ERROR: no %VERSION% package was built for:!MISSING!
    echo.
    echo   Build the solution in Release at %VERSION% first, and check for a
    echo   project-specific ^<Version^> left in one of those csprojs. If that
    echo   version is a hotfix, push that package on its own instead.
    echo.
    echo   Nothing was pushed.
    exit /b 1
)

echo Pre-flight OK: !FOUND! packages at %VERSION%.
echo.

for %%P in (%PACKAGES%) do (
    echo Pushing %%P version %VERSION%...
    dotnet nuget push .\%%P\bin\Release\%%P.%VERSION%.nupkg --source https://api.nuget.org/v3/index.json --api-key %APIKEY%
)

endlocal
