#r ".tools/FAKE/tools/FakeLib.dll"
open Fake
open Fake.AssemblyInfoFile

//download all nuget dependencies
RestorePackages()

let buildDir  = "./.build/app"
let testDir   = "./.build/test/"
let deployDir = "./.deploy/"

let kafkaNetSource = !! "src/kafka-net/*.csproj"

//version build increment
let v = SemVerHelper.parse(System.IO.File.ReadAllText("version"))
let buildNumber = System.Convert.ToInt32(v.Build) + 1
let buildVersion = [v.Major; v.Minor; v.Patch; buildNumber;] |> Seq.map System.Convert.ToString |>  String.concat(".")

Target "Record-Version-Increment" (fun _ ->
    trace ("New build number is: " + buildVersion)
    System.IO.File.WriteAllText("version", buildVersion)
)

Target "Clean" (fun _ -> 
    CleanDir buildDir
)

Target "Build-Kafka-Net" (fun _ ->
    CreateCSharpAssemblyInfo "./src/kafka-net/properties/assemblyinfo.cs"
        [Attribute.Title "kafka-net"
         Attribute.Description "Native C# client for Apache Kafka."
         Attribute.Guid "eb234ec0-d838-4abd-9224-479ca06f969d"
         Attribute.Product "kafka-net"
         Attribute.Company "James Roland"
         Attribute.Copyright "Copyright James Roland 2014"
         Attribute.Version buildVersion
         Attribute.FileVersion buildVersion]
         
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
  ==> "Record-Version-Increment"
  ==> "Build-Kafka-Net"
  ==> "Build-Kafka-Tests"
  =?> ("Run-Integration-Tests", hasBuildParam "Integration")
  =?> ("Run-Unit-Tests", hasBuildParam "Unit")
  ==> "Default"
 

RunTargetOrDefault "Default"