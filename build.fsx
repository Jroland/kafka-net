#r ".tools/FAKE/tools/FakeLib.dll"
open Fake

//download all nuget dependencies
RestorePackages()

let buildDir  = "./.build/app"
let testDir   = "./.build/test/"
let deployDir = "./.deploy/"

let kafkaNetSource = !! "src/kafka-net/*.csproj"

let version = ""

Target "Clean" (fun _ -> 
    CleanDir buildDir
)

Target "Build-Kafka-Net" (fun _ ->
    MSBuildRelease buildDir "Build" kafkaNetSource
     |> Log "AppBuild-Output: "
)

Target "Build-Kafka-Tests" (fun _ ->
    !! "src/kafka-tests/*.csproj"
      |> MSBuildDebug testDir "Build"
      |> Log "TestBuild-Output: "
)

Target "Run-Integration-Tests" (fun _ ->
    !! (testDir + "/*Tests.dll") 
      |> NUnit (fun p ->
          {p with
             DisableShadowCopy = true;
             IncludeCategory = "Integration";
             OutputFile = testDir + "IntegrationTestResults.xml" })
)

Target "Run-Unit-Tests" (fun _ ->
    !! (testDir + "/*Tests.dll") 
      |> NUnit (fun p ->
          {p with
             DisableShadowCopy = true;
             IncludeCategory = "Unit";
             OutputFile = testDir + "UnitTestResults.xml" })
)

Target "Default" (fun _ ->
    trace "Building..."
)

"Clean"
  ==> "Build-Kafka-Net"
  ==> "Build-Kafka-Tests"
  =?> ("Run-Integration-Tests",hasBuildParam "Integration")
  ==> "Run-Unit-Tests"
  ==> "Default"
 

RunTargetOrDefault "Default"