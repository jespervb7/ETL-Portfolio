[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared games = let
  Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1F4i1LoJVP8sCAxWBG-yeh64kvbeuu2XD-2Y34XvounA/edit?usp=drive_web&ouid=114517673615382305882"),
  #"Navigation 1" = Source{[name = "games", ItemKind = "Table"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"game_id", type text}, {"survivor_id", type text}, {"map_id", type text}, {"killer_id", type text}, {"item_id", type text}, {"addon_id", type text}, {"date_game_played", type date}, {"time_game_started", type time}, {"time_game_ended", type time}, {"time_of_death", type time}, {"escaped_through_hatch", type text}, {"personal_hook_stages", Int64.Type}, {"team_hook_stages", Int64.Type}, {"fresh_hooks", Int64.Type}, {"amount_of_survivors_escaped", Int64.Type}, {"generators_completed", Int64.Type}, {"blood_points", Int64.Type}, {"did_killer_quit", type text}, {"was_killer_farming", type text}, {"did_game_have_cheater", type text}, {"game_mode", type text}, {"game_type", type text}}),
  #"Filtered rows" = Table.SelectRows(#"Changed column type", each [game_id] <> null and [game_id] <> ""),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Filtered rows", {{"escaped_through_hatch", type logical}, {"was_killer_farming", type logical}, {"did_killer_quit", type logical}, {"did_game_have_cheater", type logical}, {"personal_hook_stages", Int64.Type}, {"team_hook_stages", Int64.Type}, {"fresh_hooks", Int64.Type}, {"amount_of_survivors_escaped", Int64.Type}, {"generators_completed", Int64.Type}, {"blood_points", Int64.Type}, {"time_game_started", type time}})
in
  #"Changed column type 1";
[BindToDefaultDestination = true]
shared survivors = let
  Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1F4i1LoJVP8sCAxWBG-yeh64kvbeuu2XD-2Y34XvounA/edit?usp=drive_web&ouid=114517673615382305882"),
  #"Navigation 1" = Source{[name = "survivors", ItemKind = "Table"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"survivor_id", type text}, {"survivor_ingame_name", type text}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared items = let
  Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1F4i1LoJVP8sCAxWBG-yeh64kvbeuu2XD-2Y34XvounA/edit?usp=drive_web&ouid=114517673615382305882"),
  #"Navigation 1" = Source{[name = "items", ItemKind = "Table"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"item_id", type text}, {"item_type_id", type text}, {"item_ingame_name", type text}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared item_type = let
  Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1F4i1LoJVP8sCAxWBG-yeh64kvbeuu2XD-2Y34XvounA/edit?usp=drive_web&ouid=114517673615382305882"),
  #"Navigation 1" = Source{[name = "item_type", ItemKind = "Table"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"item_type_id", type text}, {"item_name", type text}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared killers = let
  Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1F4i1LoJVP8sCAxWBG-yeh64kvbeuu2XD-2Y34XvounA/edit?usp=drive_web&ouid=114517673615382305882"),
  #"Navigation 1" = Source{[name = "killers", ItemKind = "Table"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"killer_id", type text}, {"killer_ingame_name", type text}, {"killer_image", type text}, {"created_at", type datetime}, {"created_by", type text}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared maps = let
  Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1F4i1LoJVP8sCAxWBG-yeh64kvbeuu2XD-2Y34XvounA/edit?usp=drive_web&ouid=114517673615382305882"),
  #"Navigation 1" = Source{[name = "maps", ItemKind = "Table"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"map_id", type text}, {"map_ingame_name", type text}, {"map_realm", type text}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared addons = let
  Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1F4i1LoJVP8sCAxWBG-yeh64kvbeuu2XD-2Y34XvounA/edit?usp=drive_web&ouid=114517673615382305882"),
  #"Navigation 1" = Source{[name = "addons", ItemKind = "Table"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"addon_id", type text}, {"item_type_id", type text}, {"addon_ingame_name", type text}})
in
  #"Changed column type";
shared DefaultDestination = let
  Source = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  #"Navigation 1" = Source{[workspaceId = "605b5796-17cb-4c40-a615-702a6726ae8c"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "fd9e9c25-32d5-4ecc-99ac-c5e050362b63"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "dead_by_daylight", ItemKind = "Schema"]}[Data]
in
  #"Navigation 3";
