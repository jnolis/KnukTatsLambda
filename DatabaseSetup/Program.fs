// Learn more about F# at http://fsharp.org

open System
open Amazon
open Amazon.Extensions.NETCore.Setup
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.DocumentModel
open Amazon.DynamoDBv2.Model
open Amazon.Runtime
open System.Collections.Generic
open Newtonsoft.Json

type Pair = {
    FirstWord: string
    SecondWord: string
    Count: int
}

type Keys = {
    AccessKeyId: string
    SecretAccessKey: string
}


let createClient () =
    let credentials = 
        let keys =
            "config.json"
            |> System.IO.File.ReadAllText
            |> (fun x -> Newtonsoft.Json.JsonConvert.DeserializeObject<Keys>(x))
        Amazon.Runtime.BasicAWSCredentials(accessKey = keys.AccessKeyId,secretKey=keys.SecretAccessKey);
    let config = 
        AmazonDynamoDBConfig(RegionEndpoint = RegionEndpoint.USWest2)
    new AmazonDynamoDBClient(credentials,config)

let createTables (client:AmazonDynamoDBClient) =
    let tables  = 
        client.ListTablesAsync()
        |> Async.AwaitTask 
        |> Async.RunSynchronously
    if not (Seq.contains "KnukTats.Library" tables.TableNames) then
        client.CreateTableAsync( 
            CreateTableRequest(
                        TableName = "KnukTats.Library",
                        ProvisionedThroughput = ProvisionedThroughput(ReadCapacityUnits = 1L, WriteCapacityUnits = 100L),
                        KeySchema = List [KeySchemaElement(AttributeName = "Words", KeyType = KeyType.HASH)],
                        AttributeDefinitions = 
                            List [
                                    AttributeDefinition(AttributeName = "Words", AttributeType = ScalarAttributeType.S)]
                    ))
        |> Async.AwaitTask 
        |> Async.RunSynchronously
        |> ignore
    if not (Seq.contains "KnukTats.PreviousWords" tables.TableNames) then
        client.CreateTableAsync( 
            CreateTableRequest(
                        TableName = "KnukTats.PreviousWords",
                        ProvisionedThroughput = ProvisionedThroughput(ReadCapacityUnits = 1L, WriteCapacityUnits = 100L),
                        KeySchema = List [KeySchemaElement(AttributeName = "Words", KeyType = KeyType.HASH)],
                        AttributeDefinitions = 
                            List [AttributeDefinition(AttributeName = "Words", AttributeType = ScalarAttributeType.S)]
                    ))
        |> Async.AwaitTask 
        |> Async.RunSynchronously
        |> ignore

let pairToLibraryDocument (pair:Pair) =
    let item = Document()
    do item.Add("Words", DynamoDBEntry.op_Implicit (pair.FirstWord + pair.SecondWord) )
    do item.Add("Count", DynamoDBEntry.op_Implicit pair.Count)
    item

let pairToPreviousWordsDocument (pair:Pair) =
    let item = Document()
    do item.Add("Words", DynamoDBEntry.op_Implicit (pair.FirstWord + pair.SecondWord) )
    item

let populateDatabase (client:AmazonDynamoDBClient) (pairToDocument: Pair->Document) (database: string) (pairs: Pair list) =

    let table = Table.LoadTable(client,database)
    let writer = table.CreateBatchWrite()
    
    pairs
    |> Seq.ofList
    |> Seq.map pairToDocument
    |> Seq.iter writer.AddDocumentToPut

    writer.ExecuteAsync()
    |> Async.AwaitTask 
    |> Async.RunSynchronously
    |> ignore

let lowerCapacity (client:AmazonDynamoDBClient) (tableName: string) (readCapacity:int64) (writeCapacity: int64) =
    let request = 
        UpdateTableRequest(
            TableName = tableName, 
            ProvisionedThroughput = ProvisionedThroughput(ReadCapacityUnits = readCapacity, WriteCapacityUnits = writeCapacity))
    client.UpdateTableAsync(request)
    |> Async.AwaitTask 
    |> Async.RunSynchronously
    |> ignore
let loadFile filename =
    filename
    |> System.IO.File.ReadAllLines
    |> List.ofArray
    |> List.skip 1
    |> List.choose (fun line -> 
                    try
                        let result = line.Split([|','|],3)
                        Some {FirstWord = result.[0]; SecondWord = result.[1]; Count = int result.[2]}
                    with
                    | _ -> None)
[<EntryPoint>]
let main argv =
    let library = loadFile "KnukTatsLibrary.csv"
    let previousWords = loadFile "PreviousWords.csv"
    let client = createClient()
    do createTables client
    do populateDatabase client pairToLibraryDocument "KnukTats.Library" library
    do populateDatabase client pairToPreviousWordsDocument "KnukTats.PreviousWords" previousWords
    do lowerCapacity client "KnukTats.Library" 1L 1L
    do lowerCapacity client "KnukTats.PreviousWords" 1L 1L
    0 // return an integer exit code
