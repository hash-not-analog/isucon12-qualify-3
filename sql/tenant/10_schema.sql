DROP TABLE IF EXISTS competition;

DROP TABLE IF EXISTS player;

DROP TABLE IF EXISTS player_score;

CREATE TABLE competition (
  id VARCHAR(255) NOT NULL PRIMARY KEY,
  tenant_id BIGINT NOT NULL,
  title TEXT NOT NULL,
  finished_at BIGINT NULL,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL
);

CREATE INDEX created_at_idx ON competition (created_at);

CREATE TABLE player (
  id VARCHAR(255) NOT NULL PRIMARY KEY,
  tenant_id BIGINT NOT NULL,
  display_name TEXT NOT NULL,
  is_disqualified BOOLEAN NOT NULL,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL
);

CREATE TABLE player_score (
  id VARCHAR(255) NOT NULL PRIMARY KEY,
  tenant_id BIGINT NOT NULL,
  player_id VARCHAR(255) NOT NULL,
  competition_id VARCHAR(255) NOT NULL,
  score BIGINT NOT NULL,
  row_num BIGINT NOT NULL,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL
);

CREATE INDEX tenant_idx ON player_score (tenant_id);

CREATE INDEX tenant_player_idx ON player_score (tenant_id, player_id);

CREATE INDEX tenant_player_competition_row_idx ON player_score (tenant_id, player_id, competition_id, row_num DESC);

CREATE INDEX tenant_competition_row_idx ON player_score (tenant_id, competition_id, row_num DESC);

CREATE INDEX comp_idx ON player_score (competition_id ASC);
