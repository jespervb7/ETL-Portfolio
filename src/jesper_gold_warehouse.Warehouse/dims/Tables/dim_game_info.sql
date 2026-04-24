CREATE TABLE [dims].[dim_game_info] (

	[game_info_key] int NOT NULL, 
	[escaped_through_hatch] bit NULL, 
	[did_killer_quit] bit NULL, 
	[was_killer_farming] bit NULL, 
	[did_game_have_cheater] bit NULL, 
	[game_mode] varchar(100) NULL, 
	[game_type] varchar(100) NULL, 
	[game_result] varchar(10) NULL, 
	[did_escape] bit NULL, 
	[is_valid_game] bit NULL
);