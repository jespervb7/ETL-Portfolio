[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = let
  Source = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  #"Navigation 1" = Source{[workspaceId = "605b5796-17cb-4c40-a615-702a6726ae8c"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "fd9e9c25-32d5-4ecc-99ac-c5e050362b63"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "movie_tracker", ItemKind = "Schema"]}[Data]
in
  #"Navigation 3";
[BindToDefaultDestination = true]
shared WatchLog = let
  Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1gl5PX5LriVgDNF-oAaHHvyOrbYbmjwmrr0QAn4PEoMI/edit?gid=1599716855#gid=1599716855"),
  #"Navigation 1" = Source{[name = "WatchLog", ItemKind = "Table"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"WatchID", type text}, {"MediaID", type text}, {"PlatformID", type text}, {"EpisodeID", type text}, {"TagID", type text}, {"WatchDate", type date}, {"TimeStarted", type time}, {"TimeEnded", type time}, {"CreatedAt", type text}, {"CreatedBy", type text}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared Media = let
  Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1gl5PX5LriVgDNF-oAaHHvyOrbYbmjwmrr0QAn4PEoMI/edit?gid=1599716855#gid=1599716855"),
  #"Navigation 1" = Source{[name = "Media", ItemKind = "Table"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"MediaID", type text}, {"GenreID", type text}, {"Title", type text}, {"Type", type text}, {"ReleaseYear", type date}, {"HasEpisodes", type logical}, {"CreatedAt", type text}, {"CreatedBy", type text}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared Episodes = let
  Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1gl5PX5LriVgDNF-oAaHHvyOrbYbmjwmrr0QAn4PEoMI/edit?gid=1599716855#gid=1599716855"),
  #"Navigation 1" = Source{[name = "Episodes", ItemKind = "Table"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"EpisodeID", type text}, {"MediaID", type text}, {"EpisodeTitle", type text}, {"EpisodeNumber", Int64.Type}, {"SeasonNumber", Int64.Type}, {"EpisodeInSeason", Int64.Type}, {"EpisodeCode", type text}, {"Synopsis", type text}, {"ReleaseDate", type date}, {"DurationMinutes", type time}, {"StoryArc", type text}, {"IsFiller", type text}, {"CreatedAt", type text}, {"CreatedBy", type text}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared Genres = let
  Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1gl5PX5LriVgDNF-oAaHHvyOrbYbmjwmrr0QAn4PEoMI/edit?gid=1599716855#gid=1599716855"),
  #"Navigation 1" = Source{[name = "Genres", ItemKind = "Table"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"GenreID", type text}, {"ParentGenreID", type text}, {"GenreName", type text}, {"Description", type text}, {"IsActive", type logical}, {"CreatedAt", type text}, {"CreatedBy", type text}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared Platforms = let
  Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1gl5PX5LriVgDNF-oAaHHvyOrbYbmjwmrr0QAn4PEoMI/edit?gid=1599716855#gid=1599716855"),
  #"Navigation 1" = Source{[name = "Platforms", ItemKind = "Table"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"PlatformID", type text}, {"PlatformName", type text}, {"PlatformType", type text}, {"IsStreamingService", type logical}, {"IsCinema", type logical}, {"LogoURL", type text}, {"CreatedAt", type text}, {"CreatedBy", type text}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared Tags = let
  Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1gl5PX5LriVgDNF-oAaHHvyOrbYbmjwmrr0QAn4PEoMI/edit?gid=1599716855#gid=1599716855"),
  #"Navigation 1" = Source{[name = "Tags", ItemKind = "Table"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"TagID", type text}, {"TagName", type text}, {"Category", type text}, {"Description", type text}, {"IsActive", type logical}, {"CreatedAt", type text}, {"CreatedBy", type text}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared WatchCodes = let
  Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1gl5PX5LriVgDNF-oAaHHvyOrbYbmjwmrr0QAn4PEoMI/edit?gid=1599716855#gid=1599716855"),
  #"Navigation 1" = Source{[name = "WatchCodes", ItemKind = "Table"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"WatchCodesID", type text}, {"PlatformID", type text}, {"WatchCode", type text}})
in
  #"Changed column type";
