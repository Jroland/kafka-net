cls
"nuget\nuget.exe" "install" "FAKE" "-OutputDirectory" ".tools" "-ExcludeVersion"
"nuget\nuget.exe" "install" "nunit.runners" "-OutputDirectory" ".tools" "-ExcludeVersion"
"nuget\nuget.exe" "install" "nunit" "-OutputDirectory" ".tools" "-ExcludeVersion"
pause