import pandas as pd

import pandas as pd

# Function to determine the winner and loser
def determine_winner_loser_home_away(row):
    def sets_won(scores):
        total_sets = 0
        for s in scores:
            try:
                score_home, score_away = map(int, s.split('-'))
                if score_home > score_away:
                    total_sets += 1
            except (ValueError, AttributeError):
                print(f"Skipping invalid score format: {s}")
                continue
        return total_sets

    set_scores = [str(row.get(f"Set {i} Score", "")) for i in range(1, 4)]
    
    sets_home = sets_won(set_scores)
    sets_away = len(set_scores) - sets_home
    

    if sets_home > sets_away:
        return "Home", "Away"
    elif sets_away > sets_home:
        return "Away", "Home"
    else:
        return None, None


# Function to reorganize row data
def reorganize_row_home_away(row, winner, loser):
    new_row = {}

    # General match information
    general_cols = [
        "Tournament Name", "Tournament Title", "Level", "Year", "Start Date", 
        "End Date", "Surface", "Indoor/Outdoor", "City", "Country", 
        "Singles Draw Size", "Doubles Draw Size", "Prize Money", 
        "Prize Money Currency", "Match ID", "Date", "Round Name", "Stage Type", 
        "Stage Phase", "Stage Start Date", "Stage End Date", "Group Name", 
        "Best Of", "Start Time Confirmed"
    ]
    for col in general_cols:
        new_row[col.lower().replace(" ", "_")] = row.get(col, None)

    # Mapping columns for winner/loser
    player_cols = {
        "player_name": "Player",
        "country": "Country",
        "seed": "Seed",
        "bracket_number": "Bracket Number",
    }

    for col_new, col_base in player_cols.items():
        new_row[f"winner_{col_new}"] = row.get(f"{winner} {col_base}", None)
        new_row[f"loser_{col_new}"] = row.get(f"{loser} {col_base}", None)

    # Add statistics for each set
    stat_cols = [
        "aces", "backhand_errors", "backhand_unforced_errors", "backhand_winners",
        "breakpoints_won", "double_faults", "drop_shot_unforced_errors",
        "drop_shot_winners", "first_serve_points_won", "first_serve_successful", 
        "forehand_errors", "forehand_unforced_errors", "forehand_winners", 
        "games_won", "groundstroke_errors", "groundstroke_unforced_errors", 
        "groundstroke_winners", "lob_unforced_errors", "lob_winners", 
        "max_games_in_a_row", "max_points_in_a_row", "overhead_stroke_errors",
        "overhead_stroke_unforced_errors", "overhead_stroke_winners", 
        "points_won", "points_won_from_last_10", "return_errors", 
        "return_winners", "second_serve_points_won", "second_serve_successful", 
        "service_games_won", "service_points_lost", "service_points_won", 
        "tiebreaks_won", "total_breakpoints", "volley_unforced_errors", 
        "volley_winners"
    ]

    for stat in stat_cols:
        new_row[f"winner_{stat}"] = row.get(f"{winner} {stat}", None)
        new_row[f"loser_{stat}"] = row.get(f"{loser} {stat}", None)

    # Handle scores
    for i in range(1, 4):
        new_row[f"set{i}_score"] = row.get(f"Set {i} Score", None)

    return new_row


# Transform the dataframe
def transform_home_away_data(data):
    transformed_rows = []
    
    for index, row in data.iterrows():
        
        winner, loser = determine_winner_loser_home_away(row)
        
        if winner and loser:
            transformed_row = reorganize_row_home_away(row, winner, loser)
            transformed_rows.append(transformed_row)
            


    transformed_df = pd.DataFrame(transformed_rows)


    transformed_df.to_csv('transformed_test.csv', index=False)
    
    return transformed_df
