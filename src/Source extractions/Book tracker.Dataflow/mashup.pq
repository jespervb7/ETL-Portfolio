[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = let
  Source = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  #"Navigation 1" = Source{[workspaceId = "605b5796-17cb-4c40-a615-702a6726ae8c"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "fd9e9c25-32d5-4ecc-99ac-c5e050362b63"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "book_tracker", ItemKind = "Schema"]}[Data]
in
  #"Navigation 3";
[BindToDefaultDestination = true]
shared #"Reading log" = let
  Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1gOuRbYrLzW8HkroB7JAmXcfhqu1mkVU-RgtswyxTtcI/edit?gid=0#gid=0"),
  #"Navigation 1" = Source{[name = "Reading log", ItemKind = "Table"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"date_id", type date}, {"start_time", type time}, {"end_time", type time}, {"book_id", type text}, {"page_started", Int64.Type}, {"page_ended", Int64.Type}, {"pages_read", Int64.Type}, {"duration", type time}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared Books = let
  Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1gOuRbYrLzW8HkroB7JAmXcfhqu1mkVU-RgtswyxTtcI/edit?gid=0#gid=0"),
  #"Navigation 1" = Source{[name = "Books", ItemKind = "Table"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"book_id", type text}, {"book_title", type text}, {"author_id", type text}, {"ibsn13", Int64.Type}, {"genre", type text}, {"total_pages", Int64.Type}, {"started_reading", type date}, {"finished_reading", type date}, {"series_name", type text}, {"book_image", type text}, {"reading_status", type text}, {"is_series", type logical}}),
  #"Filtered rows" = Table.SelectRows(#"Changed column type", each ([book_id] <> ""))
in
  #"Filtered rows";
[BindToDefaultDestination = true]
shared Author = let
  Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1gOuRbYrLzW8HkroB7JAmXcfhqu1mkVU-RgtswyxTtcI/edit?gid=0#gid=0"),
  #"Navigation 1" = Source{[name = "Author", ItemKind = "Table"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"author_id", type text}, {"name", type text}, {"nationality", type text}})
in
  #"Changed column type";
