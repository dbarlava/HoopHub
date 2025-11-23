import mysql.connector
from helper import helper
import streamlit as st

class db_operations():
    # constructor with MySQL connection parameters (matches connector.py)
    # You can either pass your own connection or use default parameters
    def __init__(self):
        # Load DB credentials ONLY from Streamlit Secrets
        cfg = st.secrets["db"]

        # Connect to Aiven MySQL
        self.connection = mysql.connector.connect(
            host=cfg["host"],
            port=cfg["port"],
            user=cfg["user"],
            password=cfg["password"],
            database=cfg["database"],
            ssl_mode="REQUIRED",   
        )

        self.cursor = self.connection.cursor()
        print("connection made...")

    # function to simply execute a DDL or DML query.
    # commits query, returns no results. 
    # best used for insert/update/delete queries with no parameters
    def modify_query(self, query):
        self.cursor.execute(query)
        self.connection.commit()

    # function to simply execute a DDL or DML query with parameters
    # commits query, returns no results. 
    # best used for insert/update/delete queries with named placeholders
    def modify_query_params(self, query, dictionary):
        self.cursor.execute(query, dictionary)
        self.connection.commit()

    # function to simply execute a DQL query
    # does not commit, returns results
    # best used for select queries with no parameters
    def select_query(self, query):
        result = self.cursor.execute(query)
        return result.fetchall()
    
    # function to simply execute a DQL query with parameters
    # does not commit, returns results
    # best used for select queries with named placeholders
    def select_query_params(self, query, dictionary):
        result = self.cursor.execute(query, dictionary)
        return result.fetchall()

    # function to return the value of the first row's 
    # first attribute of some select query.
    # best used for querying a single aggregate select 
    # query with no parameters
    def single_record(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]
    
    # function to return the value of the first row's 
    # first attribute of some select query.
    # best used for querying a single aggregate select 
    # query with named placeholders
    def single_record_params(self, query, dictionary):
        self.cursor.execute(query, dictionary)
        return self.cursor.fetchone()[0]
    
    # function to return a single attribute for all records 
    # from some table.
    # best used for select statements with no parameters
    def single_attribute(self, query):
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        results = [i[0] for i in results]
        results.remove(None)
        return results
    
    # function to return a single attribute for all records 
    # from some table.
    # best used for select statements with named placeholders
    def single_attribute_params(self, query, dictionary):
        self.cursor.execute(query,dictionary)
        results = self.cursor.fetchall()
        results = [i[0] for i in results]
        return results
    
    # function for bulk inserting records
    # best used for inserting many records with parameters
    def bulk_insert(self, query, data):
        self.cursor.executemany(query, data)
        self.connection.commit()

    # destructor that closes connection with DB
    def destructor(self):
        self.cursor.close()
        self.connection.close()

    def get_yesterdays_games(self):
        """
        Returns rows as:
        (game_date, home_team, away_team, home_pts, away_pts)
        """
        query = """
            SELECT 
                DATE(g.Date) AS game_date,
                ht.Name AS home_team,
                at.Name AS away_team,
                g.HomeTeamScore AS home_pts,
                g.AwayTeamScore AS away_pts
            FROM Game g
            INNER JOIN Team ht 
                ON g.HomeTeamID = ht.TeamID
            INNER JOIN Team at 
                ON g.AwayTeamID = at.TeamID
            WHERE DATE(g.Date) = CURDATE() - INTERVAL 1 DAY
            ORDER BY g.Date, home_team;
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def get_team_roster(self, team_name):
        query = """
        SELECT CONCAT(FirstName, ' ', LastName) AS PlayerName, Age, Position, Number AS JerseyNumber
        FROM Player
        WHERE TeamID = (SELECT TeamID FROM Team WHERE Name = %s)
        ORDER BY PlayerName;
        """
        self.cursor.execute(query, (team_name,))
        return self.cursor.fetchall()

    def get_team_recent_games(self, team_name):
        query = """
        SELECT DATE(g.Date) AS GameDate, ht.Name AS HomeTeam, at.Name AS AwayTeam, g.HomeTeamScore AS HomeTeamScore, g.AwayTeamScore AS AwayTeamScore
        FROM Game g
        INNER JOIN Team ht
            ON g.HomeTeamID = ht.TeamID
        INNER JOIN Team at
            ON g.AwayTeamID = at.TeamID
        WHERE ht.Name = %s OR at.Name = %s
        ORDER BY g.Date DESC
        LIMIT 5;
        """
        self.cursor.execute(query, (team_name, team_name))
        return self.cursor.fetchall()

    def get_team_bio(self, team_name):
        query = """
        SELECT 
            Team.Name,
            Team.Abbreviation,
            Team.City,
            Team.State,
            Team.Conference,
            Team.Division,
            CONCAT(HeadCoach.FirstName, ' ', HeadCoach.LastName) AS CoachName,
            Venue.Name AS VenueName
        FROM Team
        INNER JOIN HeadCoach
            ON Team.CoachID = HeadCoach.CoachID
        INNER JOIN Venue
            ON Team.VenueID = Venue.VenueID
        WHERE Team.Name = %s;
        """
        self.cursor.execute(query, (team_name,))
        return self.cursor.fetchone()
    
    def get_team_record(self, team_name):
        query = """
            SELECT
                -- Wins
                Wins,

                -- Losses
                Losses
            FROM vStandings
            WHERE Name = %s;
        """
        
        # pass same team name 4 times
        self.cursor.execute(query, (team_name,))
        return self.cursor.fetchone()
    
    def get_team_home_record(self, team_name):
        query = """
            SELECT
                SUM(IF(Game.HomeTeamScore > Game.AwayTeamScore, 1, 0)) AS HomeWins,
                SUM(IF(Game.HomeTeamScore < Game.AwayTeamScore, 1, 0)) AS HomeLosses
            FROM Game
            WHERE Game.HomeTeamID = (SELECT TeamID FROM Team WHERE Name = %s);
        """
        self.cursor.execute(query, (team_name,))
        return self.cursor.fetchone()
    
    def get_team_away_record(self, team_name):
        query = """
            SELECT
                SUM(IF(Game.AwayTeamScore > Game.HomeTeamScore, 1, 0)) AS AwayWins,
                SUM(IF(Game.AwayTeamScore < Game.HomeTeamScore, 1, 0)) AS AwayLosses
            FROM Game
            WHERE Game.AwayTeamID = (SELECT TeamID FROM Team WHERE Name = %s);
        """
        self.cursor.execute(query, (team_name,))
        return self.cursor.fetchone()

    def get_head_to_head_record(self, team_name, opponent_name):
        query = """
            SELECT
                SUM(
                    IF(
                        (Game.HomeTeamID = team1.TeamID AND Game.AwayTeamID = team2.TeamID AND Game.HomeTeamScore > Game.AwayTeamScore)
                        OR
                        (Game.AwayTeamID = team1.TeamID AND Game.HomeTeamID = team2.TeamID AND Game.AwayTeamScore > Game.HomeTeamScore),
                    1, 0)
                ) AS Wins,

                SUM(
                    IF(
                        (Game.HomeTeamID = team1.TeamID AND Game.AwayTeamID = team2.TeamID AND Game.HomeTeamScore < Game.AwayTeamScore)
                        OR
                        (Game.AwayTeamID = team1.TeamID AND Game.HomeTeamID = team2.TeamID AND Game.AwayTeamScore < Game.HomeTeamScore),
                    1, 0)
                ) AS Losses

            FROM Game
            INNER JOIN Team team1
                ON team1.Name = %s 
            INNER JOIN Team team2
                ON team2.Name = %s
            WHERE
                (Game.HomeTeamID = team1.TeamID AND Game.AwayTeamID = team2.TeamID)
            OR
                (Game.HomeTeamID = team2.TeamID AND Game.AwayTeamID = team1.TeamID);
        """

        self.cursor.execute(query, (team_name, opponent_name))
        return self.cursor.fetchone()
    
    def get_all_teams(self, team_name):
        query = """
        SELECT Name
        FROM Team
        WHERE Team.TeamID != 31 AND Team.Name != %s
        ORDER BY Team.Name ASC;
        """
        self.cursor.execute(query, (team_name,))
        rows = self.cursor.fetchall()

    # Flatten rows like [('Boston Celtics',)] → ['Boston Celtics']
        return [row[0] for row in rows]
    
    def get_team_score(self, team_name):
        query = """
            SELECT
                -- PPG: average points scored by this team
                AVG(
                    IF(Game.HomeTeamID = t.TeamID, Game.HomeTeamScore, Game.AwayTeamScore)
                ) AS PPG,

                -- Opponent PPG: average points allowed
                AVG(
                    IF(Game.HomeTeamID = t.TeamID, Game.AwayTeamScore, Game.HomeTeamScore)
                ) AS OppPPG

            FROM Game
            INNER JOIN Team t ON t.Name = %s
            WHERE Game.HomeTeamID = t.TeamID
            OR Game.AwayTeamID = t.TeamID;
        """
        self.cursor.execute(query, (team_name,))
        return self.cursor.fetchone()
    
    def get_games_by_date(self, date):
        query = """
        SELECT 
            g.GameID AS GameID,
            DATE(g.Date) AS GameDate,
            ht.Name AS HomeTeam,
            at.Name AS AwayTeam,
            g.HomeTeamScore AS HomeTeamScore,
            g.AwayTeamScore AS AwayTeamScore
        FROM Game g
        INNER JOIN Team ht
            ON g.HomeTeamID = ht.TeamID
        INNER JOIN Team at
            ON g.AwayTeamID = at.TeamID
        WHERE DATE(g.Date) = %s
        ORDER BY g.Date DESC;
        """
        self.cursor.execute(query, (date,))
        return self.cursor.fetchall()
    
    def get_boxscore(self, game_id):
        query = """
        SELECT
            team.Name AS TeamName,
            CONCAT(Player.FirstName, ' ', Player.LastName) AS PlayerName,
            stats.Minutes AS Minutes,
            stats.Points AS Points,
            stats.Rebounds AS Rebounds,
            stats.Assists AS Assists,
            stats.Blocks AS Blocks,
            stats.Steals AS Steals,
            stats.Turnovers AS Turnovers,
            stats.Fouls AS Fouls
        FROM PlayerGameStats stats
        INNER JOIN Player
            ON stats.PlayerID = Player.PlayerID
        INNER JOIN Team
            ON Player.TeamID = Team.TeamID
        WHERE stats.GameID = %s 
        ORDER BY Team.Name, stats.Minutes DESC;
        """
        self.cursor.execute(query, (game_id,))
        return self.cursor.fetchall()

    # includes all teams and free agent team to be able to add a free agent to the database
    def get_teams(self):
        query = """
        SELECT Name
        FROM Team
        ORDER BY Name ASC;
        """
        self.cursor.execute(query)
        teams = self.cursor.fetchall()

        return [team[0] for team in teams]
    
    def insert_player(self, team_name, first_name, last_name, age, position, number, height, weight):
        try:
            query = """
            INSERT INTO Player (TeamID, FirstName, LastName, Age, Position, Number, HeightInches, WeightPounds)
            VALUES ((SELECT TeamID FROM Team WHERE Name = %s), %s, %s, %s, %s, %s, %s, %s);
            """
            self.cursor.execute(query, (team_name, first_name, last_name, age, position, number, height, weight))
            self.connection.commit()
            return True
        except Exception as e:
            print(f"Error inserting player: {e}")
            return False
    
    def get_conference_standings(self, conference):
        query = """
            SELECT 
                Name,
                Wins,
                Losses,
                WinPercentage
            FROM vStandings
            WHERE Conference = %s
            ORDER BY WinPercentage DESC;
        """
        self.cursor.execute(query, (conference,))
        return self.cursor.fetchall()

    def get_league_standings(self):
        query = """
            SELECT 
                Name,
                Wins,
                Losses,
                WinPercentage
            FROM vStandings
            ORDER BY WinPercentage DESC;
        """
        self.cursor.execute(query)
        return self.cursor.fetchall() # list of tuples like [('Boston Celtics', 50, 32, 0.625), ('Brooklyn Nets', 48, 34, 0.585), ...]
    
    def get_divisions(self):
        query = """
        SELECT DISTINCT Division
        FROM Team
        WHERE TeamID != 31;
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def get_division_standings(self, division):
        query = """
        SELECT 
                Name,
                Wins,
                Losses,
                WinPercentage
            FROM vStandings
            WHERE Division = %s
            ORDER BY WinPercentage DESC;
        """
        self.cursor.execute(query, (division,))
        return self.cursor.fetchall()

    def get_all_players(self):
        query = """
        SELECT 
            Player.PlayerID, 
            CONCAT(Player.FirstName, ' ', Player.LastName) AS Name, 
            Team.Name AS Team, 
            Player.Position, 
            ROUND(AVG(PlayerGameStats.Points), 1) AS Points, 
            ROUND(AVG(PlayerGameStats.Rebounds), 1) AS Rebounds, 
            ROUND(AVG(PlayerGameStats.Assists), 1) AS Assists, 
            ROUND(AVG(PlayerGameStats.Blocks), 1) AS Blocks, 
            ROUND(AVG(PlayerGameStats.Steals), 1) AS Steals, 
            ROUND(AVG(PlayerGameStats.Turnovers), 1) AS Turnovers, 
            ROUND(AVG(PlayerGameStats.Fouls), 1) AS Fouls
        FROM PlayerGameStats
        INNER JOIN Player ON PlayerGameStats.PlayerID = Player.PlayerID
        INNER JOIN Team ON Player.TeamID = Team.TeamID
        GROUP BY Player.PlayerID
        ORDER BY Points DESC;
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def get_player_info(self, player_id):
        query = """
        SELECT
            CONCAT(Player.FirstName, ' ', Player.LastName) AS Name,
            (SELECT Team.Name FROM Team WHERE Team.TeamID = Player.TeamID) AS TeamName,
            Player.Position,
            Player.Age,
            Player.HeightInches,
            Player.WeightPounds,
            Player.Number
        FROM Player
        WHERE Player.PlayerID = %s;
        """
        self.cursor.execute(query, (player_id,))

        return self.cursor.fetchone()

