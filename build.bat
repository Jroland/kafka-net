@echo Off
set config=%1
if "%config%" == "" (
   set config=Release
)
 
set version=0.1.0
if not "%PackageVersion%" == "" (
   set version=%PackageVersion%
)
 
set nuget=
if "%nuget%" == "" (
	set nuget=nuget
)
 
%WINDIR%\Microsoft.NET\Framework\v4.0.30319\msbuild src\simple-kafka-net.sln /p:Configuration="%config%" /m /v:M /fl /flp:LogFile=msbuild.log;Verbosity=diag /nr:false
 
mkdir Build
mkdir Build\lib
mkdir Build\lib\net45
 
%nuget% pack "src\simple-kafka-net.nuspec" -NoPackageAnalysis -verbosity detailed -o Build -Version %version% -p Configuration="%config%"