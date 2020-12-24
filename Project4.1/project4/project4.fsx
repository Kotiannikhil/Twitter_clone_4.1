#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit"
#r "nuget: MathNet.Numerics.FSharp"
#r "nuget: MathNet.Numerics"

open System
open Akka
open Akka.Actor
open System.Collections.Generic
open Akka.FSharp
open System.Security.Cryptography

let system = System.create "MySystem" <| Configuration.load()

let mutable numusers=20
let mutable cparent = null
let mutable sim = null
let mutable serv = null
let mutable actorMap = Map.empty
let mutable zipfDist=Map.empty
let mutable flag1 = false
let mutable temp = true
let mutable tweetid=0
let mutable c1 = 0
let mutable c2 = 0
let mutable c3 = 0
let mutable c4 = 0
let mutable sFollower = Map.empty
let mutable sFollowing = Map.empty
let mutable tweetIdSet = Set.empty

type Twitterclone = 
    | GetMap of Map<int,Set<int>>*Map<int,Set<int>>
    | GetTweet of int*int*String
    | GetRetweet of int*int*int*int
    | Createsearchmaps of int*int*String
    | Lookupstring of int*String
    | Receive of int*Set<int>*Set<int>
    | Tweet of int
    | Retweet of int
    | GetNewsfeedTweets of int*int*int*String*int
    | DisplayNewsfeed of int
    | Performsearch of int
    | Displaysearchresults of int*String*Set<Tuple<int,String>>
    | TotalUsers of int
    | CreateUsers of int
    | Makesets of int
    | Makefollowers of int*String
    | SendMap of int
    | UserTweet of int
    | PokeSearch of int
    | LiveConnection of int
    | PokeRetweet of int
    | SingleTweet of int
    
let zipf (numusers: int)= 
    let mutable newMap = Map.empty
    let userCount = numusers
    let mutable sum = 0.0
    let mutable fli = 0.0
    let mutable followerCount = 0
    for i = 1 to userCount do
        fli <- i |> float
        fli <- 1.0 / fli
        sum <- sum + fli
    sum <- 1.0 / sum
    for i = 1 to userCount do
        fli <- i |> float
        fli <- (sum / fli)*(userCount |> float)
        followerCount <- ceil fli |> int
        newMap <- newMap.Add(i,followerCount)
    newMap

let server  (mailbox : Actor<_>) = 
    let mutable count = 0
    let mutable tweetmap = Map.empty
    let mutable retweetmap = Map.empty
    let mutable gfollower = Map.empty
    let mutable gfollowing = Map.empty
    let mutable keyword = Map.empty
    let mutable htag = Map.empty
    let mutable mentions = Map.empty
    let rec loop() = actor {
        let! msg = mailbox.Receive()
        match msg with
        |GetMap (sFollower,sFollowing) ->
            gfollower <- sFollower
            gfollowing <- sFollowing
        |GetTweet (uid,tweetid,tweet) ->
            let (tweetersid,tweet)=(uid,tweet)
            tweetmap <- tweetmap.Add(tweetid,(tweetersid,tweet))
            for i in gfollower.[uid] do
                actorMap.[i] <! GetNewsfeedTweets (tweetersid,uid,tweetid,tweet,1)
        |GetRetweet (tweetersid,tweetid,retweetersid,retweetid) ->
            retweetmap <- retweetmap.Add(retweetid,(tweetersid,retweetersid,tweetid))
            let (uid,tweet) = tweetmap.[tweetid]
            for i in gfollower.[retweetersid] do
                actorMap.[i] <! GetNewsfeedTweets (tweetersid,retweetersid,retweetid,tweet,0)
        |Createsearchmaps (uid,tweetid,tweet) ->
            let strsplit = tweet.Split ' '
            for s in strsplit do
                if s.[0] = '#' then
                    if not (htag.ContainsKey(s))  then
                        let mutable set = Set.empty
                        htag <- htag.Add(s,set)
                        htag <- htag.Add(s, htag.[s].Add(tweetid))
                    else
                        htag <- htag.Add(s, htag.[s].Add(tweetid))
                elif s.[0] = '@' then
                    if not (mentions.ContainsKey(s))  then
                        let mutable set = Set.empty
                        mentions <- mentions.Add(s,set)
                        mentions <- mentions.Add(s, mentions.[s].Add(tweetid))
                    else
                        mentions <- mentions.Add(s, mentions.[s].Add(tweetid))
                else
                    if not (keyword.ContainsKey(s))  then
                        let mutable set = Set.empty
                        keyword <- keyword.Add(s,set)
                        keyword <- keyword.Add(s, keyword.[s].Add(tweetid))
                    else
                        keyword <- keyword.Add(s, keyword.[s].Add(tweetid))                
        |Lookupstring (uid,str) ->
            let mutable res = Set.empty
            let strsplit = str.Split ' '
            for s in strsplit do
                if htag.ContainsKey(s) then
                    for tweetid in htag.[s] do
                        res <- res.Add(tweetmap.[tweetid])
                elif mentions.ContainsKey(s) then
                    for tweetid in mentions.[s] do
                        res <- res.Add(tweetmap.[tweetid])
                elif keyword.ContainsKey(s) then
                    for tweetid in keyword.[s] do
                        res <- res.Add(tweetmap.[tweetid])                
                actorMap.[uid] <! Displaysearchresults(uid,str,res)
        | Receive(_) -> None |> ignore
        | Tweet(_) -> None |> ignore
        | Retweet(_) -> None |> ignore
        | GetNewsfeedTweets(_) -> None |> ignore
        | DisplayNewsfeed(_) -> None |> ignore
        | Performsearch(_) -> None |> ignore
        | Displaysearchresults(_) -> None |> ignore
        | TotalUsers(_) -> None |> ignore
        | CreateUsers(_) -> None |> ignore
        | Makesets(_) -> None |> ignore
        | Makefollowers(_) -> None |> ignore
        | SendMap(_) -> None |> ignore
        | UserTweet(_) -> None |> ignore
        | PokeSearch(_) -> None |> ignore
        | LiveConnection(_) -> None |> ignore 
        | PokeRetweet(_) -> None |> ignore
        | SingleTweet(_) -> None |> ignore
        return! loop()
    }
    loop()

let client  (mailbox : Actor<_>) =
    let mutable tweetcount = 0
    let mutable rtweetcount = 0
    let mutable tweetTable = Map.empty
    let mutable sfollowers = Set.empty
    let mutable sfollowing = Set.empty
    let mutable timelinetweets = Set.empty
    let rec loop() = actor {
        let! msg = mailbox.Receive()
        match msg with        
        |Receive (uid,followers,following) ->
            sfollowers <- followers
            sfollowing <- following
        |Tweet (uid) ->
            tweetcount <- tweetcount + 1
            tweetid <- tweetid + 1
            
            while tweetIdSet.Contains(tweetid) do
                tweetid <- tweetid + 1
            tweetIdSet <- tweetIdSet.Add(tweetid)
            let tweetTimeStamp = System.DateTime.Now.ToString()
            let tweet= sprintf "This is my tweet number%d #User%d @User%d" tweetcount uid uid
            serv <! GetTweet (uid,tweetid,tweet)
            tweetTable<-tweetTable.Add(tweetid,tweet)
            serv <! Createsearchmaps (uid,tweetid,tweet)
        |Retweet (uid) ->
            let retweetersid = uid
            let mutable random = 0
            let mutable count=0
            let tupleCount = timelinetweets.Count
            if tupleCount = 1 then
                for tuple in timelinetweets do
                    rtweetcount <- rtweetcount + 1
                    let (tweetersid,userId,tweetid,tweet,tweetType) = tuple
                    if tweetType=1 && tweetersid <> uid && sfollowing.Contains(tweetersid) then
                        serv <! GetRetweet (tweetersid,tweetid,retweetersid,rtweetcount)
            elif tupleCount >1 then
                random<-(System.Random()).Next(1,tupleCount+1)        
                for tuple in timelinetweets do
                    count<-count+1
                    if count=random then
                        rtweetcount <- rtweetcount + 1
                        let (tweetersid,userId,tweetid,tweet,tweetType) = tuple
                        if tweetType=1 && tweetersid <> uid && sfollowing.Contains(tweetersid) then
                            serv <! GetRetweet (tweetersid,tweetid,retweetersid,rtweetcount)
        |GetNewsfeedTweets (tweetersid,userOrRetweetersid,tweetid,tweet,tweetType) ->
            timelinetweets <- timelinetweets.Add((tweetersid,userOrRetweetersid,tweetid,tweet,tweetType))
        |DisplayNewsfeed (uid) ->
            printfn "User%d is following users %A" uid sFollowing.[uid]
            printfn "Timeline for User%d" uid
            for tuple in timelinetweets do
                let (tweetersid,userOrRetweetersid,tweetid,tweet,tweetType) = tuple
                if tweetType = 1 then
                    printfn "User%d tweeted: %s" tweetersid tweet
                elif tweetType = 0 then
                    printfn "User%d retweeted User%d's tweet: %s" userOrRetweetersid tweetersid tweet
        |Performsearch (uid) ->
            printfn "What do you want to search for?"
            let str = System.Console.ReadLine()
            serv <! Lookupstring (uid,str)
        |Displaysearchresults (uid,str,res) ->
            printfn "User%d searched for string '%s' and got below result" uid str
            if res.Count = 0 then
                printfn "There are no tweets which has string '%s'." str
            else
                for tweetSet in res do
                    let (userid,tweet)=tweetSet
                    printfn "User%d : %s" userid tweet
            c2 <- c2 + 1
        | GetMap(_) -> None |> ignore
        | GetTweet(_) -> None |> ignore
        | GetRetweet(_) -> None |> ignore
        | Createsearchmaps(_) -> None |> ignore
        | Lookupstring(_) -> None |> ignore
        | TotalUsers(_) -> None |> ignore
        | CreateUsers(_) -> None |> ignore
        | Makesets(_) -> None |> ignore
        | Makefollowers(_) -> None |> ignore
        | SendMap(_) -> None |> ignore
        | UserTweet(_) -> None |> ignore
        | PokeSearch(_) -> None |> ignore
        | LiveConnection(_) -> None |> ignore
        | PokeRetweet(_) -> None |> ignore
        | SingleTweet(_) -> None |> ignore
        return! loop()
    }
    loop()

let clientParent  (mailbox : Actor<_>) = 
    let rec loop() = actor {
        let! msg = mailbox.Receive()
        match msg with
        |TotalUsers (numusers) ->
              for i = 1 to numusers do
                actorMap <- actorMap.Add(i,spawn system (sprintf "user%i" i) client)
              for i = 1 to numusers do
                sim <! Makefollowers (i,(sprintf "user%i" i))
              Threading.Thread.Sleep(500)
              sim <! SendMap (numusers)
              Threading.Thread.Sleep(500)
        | GetMap(_) -> None |> ignore
        | GetTweet(_) -> None |> ignore
        | GetRetweet(_) -> None |> ignore
        | Createsearchmaps(_) -> None |> ignore
        | Lookupstring(_) -> None |> ignore
        | Receive(_) -> None |> ignore
        | Tweet(_) -> None |> ignore
        | Retweet(_) -> None |> ignore
        | GetNewsfeedTweets(_) -> None |> ignore
        | DisplayNewsfeed(_) -> None |> ignore
        | Performsearch(_) -> None |> ignore
        | Displaysearchresults(_) -> None |> ignore
        | CreateUsers(_) -> None |> ignore
        | Makesets(_) -> None |> ignore
        | Makefollowers(_) -> None |> ignore
        | SendMap(_) -> None |> ignore
        | UserTweet(_) -> None |> ignore
        | PokeSearch(_) -> None |> ignore
        | LiveConnection(_) -> None |> ignore
        | PokeRetweet(_) -> None |> ignore
        | SingleTweet(_) -> None |> ignore
        return! loop()
    }
    loop()

let simulate  (mailbox : Actor<_>) = 
    let mutable count = 0
    let rec loop() = actor {
        let! msg = mailbox.Receive()
        match msg with
        |CreateUsers (numusers) ->
            cparent <! TotalUsers (numusers)
        |Makesets (numusers) ->
            for i = 1 to numusers do
                let mutable set = Set.empty
                sFollowing<-sFollowing.Add(i,set)        
        |Makefollowers (unum,uname)->
            let mutable ufollower = Set.empty
            let mutable r = 0
            let mutable flag65=true
            let mutable fcount=zipfDist.[unum]
            if unum < ((5*numusers)/100) then
                fcount <- 2*fcount
            else
                fcount <- 3*fcount
            for i = 1 to fcount do
                r <- (System.Random()).Next(1,numusers+1)
                while flag65 do
                    if ufollower.Contains(r) || r = unum then
                        r <- (System.Random()).Next(1,numusers+1)
                    else
                        flag65 <- false
                ufollower <- ufollower.Add(r)
                sFollower <- sFollower.Add(unum,ufollower)
                sFollowing <- sFollowing.Add(r,sFollowing.[r].Add(unum))
                flag65 <- true
            actorMap.[unum] <! Receive(unum,ufollower,sFollowing.[unum])
        |SendMap (numusers) ->
            serv <! GetMap(sFollower,sFollowing)
        |UserTweet (numusers)->
            for i = 1 to numusers do
                c1 <- c1 + 1
                actorMap.[i] <! Tweet (i)
                Threading.Thread.Sleep(50)
            Threading.Thread.Sleep(400)
        |PokeSearch (userId) ->
            actorMap.[userId] <! Performsearch (userId)
        |PokeRetweet (numusers) ->
            for userId = 1 to numusers do
                c3 <- c3 + 1
                if sFollowing.[userId].IsEmpty then
                    count <- count+1
                    Threading.Thread.Sleep(50)
                else
                    actorMap.[userId] <! Retweet (userId)
                    count <- count+1
                    Threading.Thread.Sleep(50)
            Threading.Thread.Sleep(1000)
        |SingleTweet (numusers) ->
            let r = (System.Random()).Next(1,numusers+1)
            printfn "The user that goes live is: %d" r
            printfn "The user tweets but offline followers cannot see the tweet"
            let mutable numf = 0
            for i in sFollower.[r] do
                if numf = 0 then
                    numf <- numf + 1
                    actorMap.[i] <! DisplayNewsfeed (i)
                    Threading.Thread.Sleep(100)
            actorMap.[r] <! Tweet (r)
            Threading.Thread.Sleep(100)
            printfn "Making followers of %d online and printing their newsfeeds" r
            for i in sFollower.[r] do
                if numf = 1 then
                    numf <- numf + 1
                    actorMap.[i] <! DisplayNewsfeed (i)
                    Threading.Thread.Sleep(100)
            c4 <- c4 + 1
        | GetMap(_) -> None |> ignore
        | GetTweet(_) -> None |> ignore
        | GetRetweet(_) -> None |> ignore
        | Createsearchmaps(_) -> None |> ignore
        | Lookupstring(_) -> None |> ignore
        | Receive(_) -> None |> ignore
        | Tweet(_) -> None |> ignore
        | Retweet(_) -> None |> ignore
        | GetNewsfeedTweets(_) -> None |> ignore
        | DisplayNewsfeed(_) -> None |> ignore
        | Performsearch(_) -> None |> ignore
        | Displaysearchresults(_) -> None |> ignore
        | TotalUsers(_) -> None |> ignore
        | LiveConnection(_) -> None |> ignore 
        return! loop()
    }
    loop()
let mutable flag = false
numusers <- int fsi.CommandLineArgs.[1]
zipfDist <- zipf numusers
cparent <- spawn system "clientParent" clientParent
serv <- spawn system "server" server
sim <- spawn system "simulate" simulate
sim <! Makesets (numusers)
sim <! CreateUsers (numusers)
Threading.Thread.Sleep(3000)
let mutable stopWatch = System.Diagnostics.Stopwatch.StartNew()
sim <! UserTweet (numusers)
while flag = false do
    if c1 = numusers then
        flag <- true
stopWatch.Stop()
printfn "Time taken for users to tweet is %f milliseconds" stopWatch.Elapsed.TotalMilliseconds


Threading.Thread.Sleep(3000)
flag <- false
stopWatch <- System.Diagnostics.Stopwatch.StartNew()
sim <! PokeSearch (1)
while flag = false do
    if c2 = 1 then
        flag <- true
stopWatch.Stop()
printfn "Time taken for user to search is %f milliseconds" stopWatch.Elapsed.TotalMilliseconds

Threading.Thread.Sleep(3000)
flag <- false
stopWatch <- System.Diagnostics.Stopwatch.StartNew()
sim <! PokeRetweet (numusers)
while flag = false do
    if c3 = numusers then
        flag <- true
stopWatch.Stop()
printfn "Time taken for users to retweet is %f milliseconds" stopWatch.Elapsed.TotalMilliseconds

Threading.Thread.Sleep(3000)
flag <- false
stopWatch <- System.Diagnostics.Stopwatch.StartNew()
sim <! SingleTweet (numusers)
while flag = false do
    if c4 = 1 then
        flag <- true
stopWatch.Stop()
printfn "Time taken for a single user to tweet is %f milliseconds" stopWatch.Elapsed.TotalMilliseconds
