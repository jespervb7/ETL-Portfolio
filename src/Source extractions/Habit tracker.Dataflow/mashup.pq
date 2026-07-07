[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared habit_log = let
  Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1E-lVFr-d16AfJt7vW92CHiFMwAWQ0oRbUgL7TDZTr9k/edit?gid=778429712#gid=778429712"),
  #"Navigation 1" = Source{[name = "habit_log", ItemKind = "Table"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"habit_log_id", type text}, {"habit_id", type text}, {"completed_at", type datetime}, {"modified_at", type datetime}, {"created_at", type datetime}, {"habit_date", type date}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared habits = let
  Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1E-lVFr-d16AfJt7vW92CHiFMwAWQ0oRbUgL7TDZTr9k/edit?gid=778429712#gid=778429712"),
  #"Navigation 1" = Source{[name = "habits", ItemKind = "Table"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"habit_id", type text}, {"habit_name", type text}, {"habit_occurance", type text}, {"habit_target_per_period", Int64.Type}, {"habit_category", type text}, {"habit_is_active", type logical}, {"modified_at", type datetime}, {"created_at", type datetime}})
in
  #"Changed column type";
shared DefaultDestination = let
  Source = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  #"Navigation 1" = Source{[workspaceId = "605b5796-17cb-4c40-a615-702a6726ae8c"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "fd9e9c25-32d5-4ecc-99ac-c5e050362b63"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "habit_tracker", ItemKind = "Schema"]}[Data]
in
  #"Navigation 3";
