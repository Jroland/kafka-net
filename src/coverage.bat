.\packages\OpenCover.4.5.3723\OpenCover.Console.exe -register:user "-filter:+[SimpleKafka]* -[*Test]*" "-target:.\packages\NUnit.Console.3.0.0-beta-1\tools\nunit-console.exe" "-targetargs:.\SimpleKafkaTests\bin\Debug\SimpleKafkaTests.dll"

.\packages\ReportGenerator.2.1.4.0\ReportGenerator.exe "-reports:results.xml" "-targetdir:.\coverage"

pause
