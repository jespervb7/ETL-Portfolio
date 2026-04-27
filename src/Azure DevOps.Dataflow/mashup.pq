[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared Areas = let
  Source = OData.Feed("https://analytics.dev.azure.com/jespervanbeemdelust/Jesper%20data%20solutions/_odata/v3.0-preview", null, [Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "Areas", Signature = "table"]}[Data]
in
  #"Navigation 1";
shared BoardLocations = let
  Source = OData.Feed("https://analytics.dev.azure.com/jespervanbeemdelust/Jesper%20data%20solutions/_odata/v3.0-preview", null, [Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "BoardLocations", Signature = "table"]}[Data]
in
  #"Navigation 1";
shared Branches = let
  Source = OData.Feed("https://analytics.dev.azure.com/jespervanbeemdelust/Jesper%20data%20solutions/_odata/v3.0-preview", null, [Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "Branches", Signature = "table"]}[Data]
in
  #"Navigation 1";
shared Dates = let
  Source = OData.Feed("https://analytics.dev.azure.com/jespervanbeemdelust/Jesper%20data%20solutions/_odata/v3.0-preview", null, [Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "Dates", Signature = "table"]}[Data]
in
  #"Navigation 1";
shared Iterations = let
  Source = OData.Feed("https://analytics.dev.azure.com/jespervanbeemdelust/Jesper%20data%20solutions/_odata/v3.0-preview", null, [Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "Iterations", Signature = "table"]}[Data]
in
  #"Navigation 1";
shared Processes = let
  Source = OData.Feed("https://analytics.dev.azure.com/jespervanbeemdelust/Jesper%20data%20solutions/_odata/v3.0-preview", null, [Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "Processes", Signature = "table"]}[Data]
in
  #"Navigation 1";
shared Projects = let
  Source = OData.Feed("https://analytics.dev.azure.com/jespervanbeemdelust/Jesper%20data%20solutions/_odata/v3.0-preview", null, [Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "Projects", Signature = "table"]}[Data]
in
  #"Navigation 1";
shared Tags = let
  Source = OData.Feed("https://analytics.dev.azure.com/jespervanbeemdelust/Jesper%20data%20solutions/_odata/v3.0-preview", null, [Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "Tags", Signature = "table"]}[Data]
in
  #"Navigation 1";
shared Teams = let
  Source = OData.Feed("https://analytics.dev.azure.com/jespervanbeemdelust/Jesper%20data%20solutions/_odata/v3.0-preview", null, [Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "Teams", Signature = "table"]}[Data]
in
  #"Navigation 1";
shared Users = let
  Source = OData.Feed("https://analytics.dev.azure.com/jespervanbeemdelust/Jesper%20data%20solutions/_odata/v3.0-preview", null, [Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "Users", Signature = "table"]}[Data]
in
  #"Navigation 1";
shared WorkItemBoardSnapshot = let
  Source = OData.Feed("https://analytics.dev.azure.com/jespervanbeemdelust/Jesper%20data%20solutions/_odata/v3.0-preview", null, [Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "WorkItemBoardSnapshot", Signature = "table"]}[Data]
in
  #"Navigation 1";
shared WorkItemLinks = let
  Source = OData.Feed("https://analytics.dev.azure.com/jespervanbeemdelust/Jesper%20data%20solutions/_odata/v3.0-preview", null, [Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "WorkItemLinks", Signature = "table"]}[Data]
in
  #"Navigation 1";
shared WorkItemRevisions = let
  Source = OData.Feed("https://analytics.dev.azure.com/jespervanbeemdelust/Jesper%20data%20solutions/_odata/v3.0-preview", null, [Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "WorkItemRevisions", Signature = "table"]}[Data]
in
  #"Navigation 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "WorkItems_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared WorkItems = let
  Source = OData.Feed("https://analytics.dev.azure.com/jespervanbeemdelust/Jesper%20data%20solutions/_odata/v3.0-preview", null, [Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "WorkItems", Signature = "table"]}[Data]
in
  #"Navigation 1";
shared WorkItemSnapshot = let
  Source = OData.Feed("https://analytics.dev.azure.com/jespervanbeemdelust/Jesper%20data%20solutions/_odata/v3.0-preview", null, [Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "WorkItemSnapshot", Signature = "table"]}[Data]
in
  #"Navigation 1";
shared WorkItemTypeFields = let
  Source = OData.Feed("https://analytics.dev.azure.com/jespervanbeemdelust/Jesper%20data%20solutions/_odata/v3.0-preview", null, [Implementation = "2.0"]),
  #"Navigation 1" = Source{[Name = "WorkItemTypeFields", Signature = "table"]}[Data]
in
  #"Navigation 1";
shared WorkItems_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "605b5796-17cb-4c40-a615-702a6726ae8c"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "fd9e9c25-32d5-4ecc-99ac-c5e050362b63"]}[Data],
  TableNavigation = Navigation_2{[Id = "WorkItems", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
