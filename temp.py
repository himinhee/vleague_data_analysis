import dao
import pandas as pd

hometeam="IB"
#column_list=['player_id','current_team','player','backnum','position_real']
allplayers=dao.readall("code_player")

allplayers_df=pd.DataFrame(allplayers, columns=['player_id','current_team','player','backnum','position_real'])
print(allplayers_df)
#print(allplayers_df['backnum'])
temp = allplayers_df[allplayers_df.current_team == hometeam]
print(temp)