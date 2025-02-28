


training_data_file_path <- "AO_2023_Training_file.csv"
testing_data_file_path <- "AO_2023_Test_file.csv"


###################################################################

# load packages

library(tidyverse)
library(doFuture)
library(furrr)
library(tidymodels)
library(vip)

options(tidymodels.dark = TRUE)


###################################################################

# import training and testing data and append

train_and_test_appended <- 
  read_csv(training_data_file_path) %>%
  bind_rows(
    
    read_csv(testing_data_file_path) %>%
      mutate(is_test_row = is.na(zone)) %>%
      group_by(is_test_row) %>% 
      mutate(test_row_index = if_else(is_test_row, row_number(), NA_real_)) %>% 
      ungroup() %>% 
      select(-is_test_row)
    
  )


# train_and_test_appended %>% 
#   distinct(match_id)


###################################################################

# this object performs a "switching" function
# that allows us to switch between running the process with the actual test rows 
# or a collection of pseudo test rows, which we use to validate our models performance

rows_to_use_for_testing <- sym("test_row_index")



###################################################################



if (FALSE) {
  
  # Below is an alternative process for generating train_and_test_appended
  # this way also generates a validation set ("pesudo test rows")
  # which allows us to evaluate our prediction methods used
  
  test_file_with_modifications <-
    read_csv(testing_data_file_path) %>%
    mutate(is_test_row = is.na(zone),
           is_not_straight_after_an_actual_test_row = !(lag(is_test_row) == TRUE  | lag(is_test_row, 2) == TRUE),
           is_not_in_first_10_rows = row_number() > 10,
           meets_criteria_for_pseudo_test_row = (
             zone != -1 &
               !is_test_row &
               is_not_straight_after_an_actual_test_row &
               is_not_straight_after_an_actual_test_row &
               is_not_in_first_10_rows
           )) %>%
    select(-is_not_straight_after_an_actual_test_row, -is_not_in_first_10_rows) %>%
    group_by(meets_criteria_for_pseudo_test_row) %>%
    mutate(is_pseudo_test_row = meets_criteria_for_pseudo_test_row & (row_number() %% 4) == 1) %>%
    group_by(is_pseudo_test_row) %>%
    mutate(pseudo_test_row_index = if_else(is_pseudo_test_row, row_number(), NA_real_)) %>%
    select(-meets_criteria_for_pseudo_test_row) %>%
    group_by(is_test_row) %>%
    mutate(test_row_index = if_else(is_test_row, row_number(), NA_real_)) %>%
    ungroup()
  
  
  train_and_test_appended <-
    read_csv(training_data_file_path) %>%
    mutate(from_training_data_file = TRUE) %>%
    bind_rows(test_file_with_modifications)
  
  
  rows_to_use_for_testing <- sym("pseudo_test_row_index")
  
  
}


# we now isolates the rows which will require predictions

all_test_rows <- 
  train_and_test_appended %>% 
  mutate(test_row_index_to_use = {{rows_to_use_for_testing}}, .before = 1) %>% 
  filter(!is.na(test_row_index_to_use)) %>% 
  select(-any_of(c("test_row_index", "pseudo_test_row_index", "from_training_data_file", "is_test_row", "is_pseudo_test_row"))) %>% 
  mutate(zone = factor(zone)) 



###################################################################


# this function isolates the training data we would have available for any particular test point

isolate_training_data <- function(train_test_combination_with_extra_info_cols, specific_test_row_index, rows_to_use_for_testing) {
  
  # specific_test_row_index <- 586
  # rows_to_use_for_testing <- sym("pseudo_test_row_index")
  # rows_to_use_for_testing <- sym("test_row_index")
  
  # train_test_combination_with_extra_info_cols <- train_and_test_appended
  
  
  complete_set_of_training_data_rows_for_sepecified_test_row <-
    train_test_combination_with_extra_info_cols %>%
    mutate(test_row_index_to_use = {{rows_to_use_for_testing}}, .before = 1) %>% 
    select(-any_of(c("test_row_index", "pseudo_test_row_index", "from_training_data_file", "is_test_row", "is_pseudo_test_row"))) %>% 
    mutate(filled_up_test_row_index = test_row_index_to_use) %>% 
    fill(filled_up_test_row_index, .direction = "up") %>% 
    filter(filled_up_test_row_index <= specific_test_row_index) %>% 
    select(-filled_up_test_row_index) %>% 
    filter(is.na(test_row_index_to_use) | test_row_index_to_use != max(test_row_index_to_use, na.rm = T)) %>% # exclude the test row in question
    select(-test_row_index_to_use) %>% 
    filter(zone != -1) %>% 
    mutate(zone = factor(zone))
  
  
  return(complete_set_of_training_data_rows_for_sepecified_test_row)
  
}



# this function makes rates from the training data
# using certain feature columns

make_training_data_observed_rates <- function(training_data_to_use, cols_to_group_by = NULL) {
  
  # training_data_to_use <- isolate_training_data(train_and_test_appended, 1, test_row_type_col_val) 
  # 
  # cols_to_group_by <- NULL
  # 
  # cols_to_group_by <- c(
  #   "gender",
  #   "serve",
  #   "court_side",
  #   "receiver_handedness",
  #   "server_handedness"
  # )
  
  standard_training_data_rates <- 
    training_data_to_use %>%  
    group_by(across(c(zone, all_of(cols_to_group_by)))) %>% 
    summarise(n = n()) %>% 
    group_by(across(all_of(cols_to_group_by))) %>% 
    mutate(total_rallies = sum(n)) %>% 
    ungroup() %>% 
    mutate(percent_of_total_rallies = n/total_rallies) %>% 
    select(-n) %>% 
    spread(zone, percent_of_total_rallies, fill = 0) %>% 
    arrange(total_rallies)
  
  
  return(standard_training_data_rates)
  
}


###################################################################
###################################################################

# in this section we determine the historical rates of the various zones 
# according the simple features identified 

local_cores <- parallelly::availableCores()
local_workers <- parallelly::makeClusterPSOCK(local_cores)
registerDoFuture()
plan(cluster, workers = local_workers) 


tictoc::tic()

part_1 <- 
  all_test_rows %>%  
  nest(test_point_data = -test_row_index_to_use) %>% 
  mutate(observed_zone_rates_from_preceding_points = future_map(test_row_index_to_use, ~.x %>% 
                                                                  isolate_training_data(train_and_test_appended, ., {{rows_to_use_for_testing}}) %>% 
                                                                  make_training_data_observed_rates(cols_to_group_by = c("gender", 
                                                                                                                         "serve", 
                                                                                                                         "court_side", 
                                                                                                                         "receiver_handedness", 
                                                                                                                         "server_handedness"))
                                                                , .progress = T, .options = furrr_options(conditions = character()))) 
tictoc::toc()

# 63.07 sec elapsed






simple_feature_rates_for_each_test_row <- 
  part_1 %>% 
  mutate(prediciton_values_that_apply_to_row = map2(test_point_data, observed_zone_rates_from_preceding_points, ~.x %>% 
                                                      left_join(.y, by = join_by(gender, court_side, serve, server_handedness, receiver_handedness)))) %>% 
  select(-observed_zone_rates_from_preceding_points, test_point_data) %>% 
  unnest(prediciton_values_that_apply_to_row) %>% 
  select(test_row_index_to_use, zone, total_rallies, as.character(0:5))







if (FALSE) {
  
  simple_feature_rates_for_each_test_row %>% 
    yardstick::mn_log_loss(zone, `0`:`5`) %>% 
    pluck(".estimate")
  
  # [1] 1.590069
  
}



###################################################################

# this section calculate rates, as above..
# but these rates are only using historical data from the server in question (for each specific test point)


local_cores <- parallelly::availableCores()
local_workers <- parallelly::makeClusterPSOCK(local_cores)
registerDoFuture()
plan(cluster, workers = local_workers) 



tictoc::tic()  

part_1 <- 
  all_test_rows %>%  
  nest(test_point_data = -test_row_index_to_use) %>% 
  mutate(observed_zone_rates_from_preceding_points = future_map(test_row_index_to_use, ~.x %>% 
                                                                  isolate_training_data(train_and_test_appended, ., {{rows_to_use_for_testing}}) %>% 
                                                                  filter(server_id == all_test_rows[[.x, "server_id"]]) %>% 
                                                                  make_training_data_observed_rates(cols_to_group_by = c("serve", 
                                                                                                                         "court_side", 
                                                                                                                         "receiver_handedness"))
                                                                , .progress = T, .options = furrr_options(conditions = character()))) 
tictoc::toc()

# 61.56 sec elapsed






# we combine the rates (per server) with the previously established overall rates (ie, using predictions from the previous section)

combination_of_rates_by_server <- 
  part_1 %>% 
  mutate(prediciton_values_that_apply_to_row = map2(test_point_data, observed_zone_rates_from_preceding_points, ~.x %>% 
                                                      left_join(.y, by = join_by(court_side, serve, receiver_handedness)))) %>% 
  select(-observed_zone_rates_from_preceding_points, test_point_data) %>% 
  unnest(prediciton_values_that_apply_to_row) %>% 
  select(test_row_index_to_use, zone, total_rallies, as.character(0:5)) %>% 
  bind_rows(simple_feature_rates_for_each_test_row) %>% 
  gather(zone_for_pred, value, -test_row_index_to_use, -zone, -total_rallies) %>% 
  group_by(test_row_index_to_use, zone, zone_for_pred) %>% 
  summarise(prediction_value = mean(value, na.rm = T)) %>% 
  group_by(test_row_index_to_use, zone) %>%
  mutate(prediction_value = prediction_value/sum(prediction_value)) %>% 
  ungroup() %>% 
  spread(zone_for_pred, prediction_value)






if (FALSE) {
  
  combination_of_rates_by_server %>% 
    yardstick::mn_log_loss(zone, `0`:`5`) %>% 
    pluck(".estimate")
  
  # [1] 1.56258
  
}



###################################################################
###################################################################

# for our xgboost model we want to provide some more interesting features
# the main goal of the feature set was to indicate the point's "gravitas"
# in order to get this we needed to reverse engineer the cumualative games and sets won for each player for each point 


make_the_features <- function(train_and_test_appended) {
  
  min_score_to_win_in_standard_non_game_deciding_tie_breaker <- 7
  min_score_to_win_in_game_deciding_tie_breaker <- 10
  games_to_win_set <- 6
  
  
  match_ordering <- 
    train_and_test_appended %>% 
    distinct(match_id) %>% 
    mutate(match_order = row_number()) %>% 
    select(match_id, match_order)
  
  
  unique_players_by_match <- 
    train_and_test_appended %>% 
    distinct(match_id, server_id, receiver_id) %>% 
    gather(key, player_id, -match_id) %>% 
    distinct(match_id, player_id)
  
  
  
  players_by_match <- 
    unique_players_by_match %>% 
    arrange(player_id) %>% 
    group_by(match_id) %>% 
    mutate(column = paste0("player_", row_number())) %>% 
    ungroup() %>% 
    spread(column, player_id) 
  
  
  
  match_number_for_player <- 
    unique_players_by_match %>% 
    left_join(match_ordering, by = join_by(match_id)) %>% 
    arrange(player_id, match_order) %>% 
    group_by(player_id) %>% 
    mutate(match_number_for_player = row_number()) %>% 
    ungroup() %>% 
    select(-match_order) %>% 
    arrange(player_id) %>% 
    group_by(match_id) %>% 
    mutate(column = paste0("match_number_for_player_", row_number())) %>% 
    ungroup() %>% 
    select(-player_id) %>% 
    spread(column, match_number_for_player)
  
  
  
  
  
  
  
  score_related_features <- 
    train_and_test_appended %>% 
    mutate(outcome = case_when(row_number() == max(row_number()) ~ outcome,
                               serve == 1 & lead(serve) == 2 & outcome != "fault" ~ paste0("fault (manual-correction from: ", outcome, ")"), 
                               TRUE ~ outcome)) %>% 
    
    mutate(reciever_court_side = case_when(is.na(receiver_position_x) ~ "no x coord", 
                                           receiver_position_x < 0 ~ "left", 
                                           TRUE ~ "right"),
           receiver_position_x = abs(receiver_position_x)) %>% 
    
    
    left_join(players_by_match, by = join_by(match_id)) %>% 
    
    left_join(match_number_for_player, by = join_by(match_id)) %>% 
    
    
    mutate(match_number_for_server = if_else(server_id == player_1, match_number_for_player_1, match_number_for_player_2),
           match_number_for_receiver = if_else(receiver_id == player_1, match_number_for_player_1, match_number_for_player_2)) %>% 
    select(-match_number_for_player_1, -match_number_for_player_2) %>% 
    
    
    
    
    
    mutate(potential_game_winning_rally_for_server = (server_score == "40" & !receiver_score %in% c("40", "AD")) | server_score == "AD",
           potential_game_winning_rally_for_receiver = (receiver_score == "40" & !server_score %in% c("40", "AD")) | receiver_score == "AD",
           
           standard_game_server_win = if_else(potential_game_winning_rally_for_server & point_winner == server_id & !(str_starts(outcome, "fault") & serve == 1), server_id, NA_character_),
           standard_game_receiver_win = if_else(potential_game_winning_rally_for_receiver & point_winner == receiver_id & !(str_starts(outcome, "fault") & serve == 1), receiver_id, NA_character_)) %>% 
    
    mutate(rally_potential_status = 
             case_when(
               !potential_game_winning_rally_for_server & !potential_game_winning_rally_for_receiver ~ NA_character_,
               potential_game_winning_rally_for_server ~ "potential standard game win for server", 
               potential_game_winning_rally_for_receiver ~ "potential standard game win for reciever")) %>% 
    
    
    ###################################################################
  # have to determine tie-breakers this way because historical games won is sometimes missing games / ie, the count of games past is incomplete
  # there migth be a neater way of working this out (ie using the game numbers in the set)
  
  mutate(across(c(server_score, receiver_score), list(numeric = ~.x %>% as.numeric()))) %>% 
    group_by(match_id, set_number) %>% 
    mutate(game_is_a_set_tie_breaker = ((any(server_score_numeric == 1) | any(receiver_score_numeric == 1)) & game_number == max(game_number)) %>% replace_na(FALSE)) %>% # this is okay it just won't work for 0-0... and will give it a  
    ungroup() %>% 
    
    
    mutate(potential_game_winning_rally_for_server = game_is_a_set_tie_breaker & ((server_score_numeric == (min_score_to_win_in_standard_non_game_deciding_tie_breaker-1) & receiver_score_numeric <= (min_score_to_win_in_standard_non_game_deciding_tie_breaker-2)) | (server_score_numeric >= min_score_to_win_in_standard_non_game_deciding_tie_breaker & receiver_score_numeric < server_score_numeric)),
           potential_game_winning_rally_for_receiver = game_is_a_set_tie_breaker & ((receiver_score_numeric == (min_score_to_win_in_standard_non_game_deciding_tie_breaker-1) & server_score_numeric <= (min_score_to_win_in_standard_non_game_deciding_tie_breaker-2)) | (receiver_score_numeric >= min_score_to_win_in_standard_non_game_deciding_tie_breaker & server_score_numeric < receiver_score_numeric)),
           
           tie_breaker_server_win = if_else(potential_game_winning_rally_for_server & point_winner == server_id & !(str_starts(outcome, "fault") & serve == 1), server_id, NA_character_),
           tie_breaker_receiver_win  = if_else(potential_game_winning_rally_for_receiver & point_winner == receiver_id & !(str_starts(outcome, "fault") & serve == 1), receiver_id, NA_character_)) %>% 
    
    select(-server_score_numeric, -receiver_score_numeric) %>% 
    
    mutate(rally_potential_status = case_when(
      !is.na(rally_potential_status) ~ rally_potential_status,
      potential_game_winning_rally_for_server ~ "potential tie-breaker game win for server", 
      potential_game_winning_rally_for_receiver ~ "potential tie-breaker game win for reciever",
      TRUE ~ "not a potential game winning rally")) %>% 
    
    select(-potential_game_winning_rally_for_server, -potential_game_winning_rally_for_receiver) %>% 
    
    # this works out how many games the server/receiver has won over the match so far
    group_by(match_id) %>% 
    mutate(
      player_1_cumulative_games_won_as_server = if_else(standard_game_server_win == player_1 | tie_breaker_server_win == player_1, 1, NA_real_) %>% replace_na(0) %>% cumsum(),
      player_1_cumulative_games_won_as_receiver = if_else(standard_game_receiver_win == player_1 | tie_breaker_receiver_win == player_1, 1, NA_real_) %>% replace_na(0) %>% cumsum(),
      player_1_total_cumulative_games_won = player_1_cumulative_games_won_as_server + player_1_cumulative_games_won_as_receiver,
      
      player_2_cumulative_games_won_as_server = if_else(standard_game_server_win == player_2 | tie_breaker_server_win == player_2, 1, NA_real_) %>% replace_na(0) %>% cumsum(),
      player_2_cumulative_games_won_as_receiver = if_else(standard_game_receiver_win == player_2 | tie_breaker_receiver_win == player_2, 1, NA_real_) %>% replace_na(0) %>% cumsum(),
      player_2_total_cumulative_games_won = player_2_cumulative_games_won_as_server + player_2_cumulative_games_won_as_receiver) %>% 
    
    mutate(
      servers_cumulative_games_won_as_server = if_else(server_id == player_1, lag(player_1_cumulative_games_won_as_server), lag(player_2_cumulative_games_won_as_server)) %>% replace_na(0),
      servers_cumulative_games_won_as_receiver = if_else(server_id == player_1, lag(player_1_cumulative_games_won_as_receiver), lag(player_2_cumulative_games_won_as_receiver)) %>% replace_na(0),
      servers_total_cumulative_games_won = if_else(server_id == player_1, lag(player_1_total_cumulative_games_won), lag(player_2_total_cumulative_games_won)) %>% replace_na(0),
      
      receivers_cumulative_games_won_as_server = if_else(receiver_id == player_1, lag(player_1_cumulative_games_won_as_server), lag(player_2_cumulative_games_won_as_server)) %>% replace_na(0),
      receivers_cumulative_games_won_as_receiver = if_else(receiver_id == player_1, lag(player_1_cumulative_games_won_as_receiver), lag(player_2_cumulative_games_won_as_receiver)) %>% replace_na(0),
      receivers_total_cumulative_games_won = if_else(receiver_id == player_1, lag(player_1_total_cumulative_games_won), lag(player_2_total_cumulative_games_won)) %>% replace_na(0)) %>% 
    
    # this works out how many games the server/receiver has won over the current set
    group_by(match_id, set_number) %>% 
    
    mutate(
      player_1_cumulative_games_won_as_server = if_else(standard_game_server_win == player_1 | tie_breaker_server_win == player_1, 1, NA_real_) %>% replace_na(0) %>% cumsum(),
      player_1_cumulative_games_won_as_receiver = if_else(standard_game_receiver_win == player_1 | tie_breaker_receiver_win == player_1, 1, NA_real_) %>% replace_na(0) %>% cumsum(),
      player_1_total_cumulative_games_won = player_1_cumulative_games_won_as_server + player_1_cumulative_games_won_as_receiver,
      
      player_2_cumulative_games_won_as_server = if_else(standard_game_server_win == player_2 | tie_breaker_server_win == player_2, 1, NA_real_) %>% replace_na(0) %>% cumsum(),
      player_2_cumulative_games_won_as_receiver = if_else(standard_game_receiver_win == player_2 | tie_breaker_receiver_win == player_2, 1, NA_real_) %>% replace_na(0) %>% cumsum(),
      player_2_total_cumulative_games_won = player_2_cumulative_games_won_as_server + player_2_cumulative_games_won_as_receiver) %>% 
    
    mutate(
      servers_cumulative_games_won_as_server_this_set = if_else(server_id == player_1, lag(player_1_cumulative_games_won_as_server), lag(player_2_cumulative_games_won_as_server)) %>% replace_na(0),
      servers_cumulative_games_won_as_receiver_this_set = if_else(server_id == player_1, lag(player_1_cumulative_games_won_as_receiver), lag(player_2_cumulative_games_won_as_receiver)) %>% replace_na(0),
      servers_total_cumulative_games_won_this_set = servers_cumulative_games_won_as_server_this_set + servers_cumulative_games_won_as_receiver_this_set,
      
      receivers_cumulative_games_won_as_server_this_set = if_else(receiver_id == player_1, lag(player_1_cumulative_games_won_as_server), lag(player_2_cumulative_games_won_as_server)) %>% replace_na(0),
      receivers_cumulative_games_won_as_receiver_this_set = if_else(receiver_id == player_1, lag(player_1_cumulative_games_won_as_receiver), lag(player_2_cumulative_games_won_as_receiver)) %>% replace_na(0),
      receivers_total_cumulative_games_won_this_set = receivers_cumulative_games_won_as_server_this_set + receivers_cumulative_games_won_as_receiver_this_set) %>% 
    ungroup() %>% 
    
    mutate(first_row_of_the_set = game_number == 1 & point == 1) %>% 
    group_by(match_id) %>% 
    mutate(
      player_1_cumulative_sets_won = if_else(first_row_of_the_set & (lag(player_1_total_cumulative_games_won) > lag(player_2_total_cumulative_games_won)), 1, NA_real_) %>% replace_na(0) %>% cumsum(),
      player_2_cumulative_sets_won = if_else(first_row_of_the_set & (lag(player_2_total_cumulative_games_won) > lag(player_1_total_cumulative_games_won)), 1, NA_real_) %>% replace_na(0) %>% cumsum(),
    ) %>%
    select(-first_row_of_the_set) %>%
    mutate(  
      servers_cumulative_sets_won = if_else(server_id == player_1, player_1_cumulative_sets_won, player_2_cumulative_sets_won) %>% replace_na(0),
      receivers_cumulative_sets_won = if_else(receiver_id == player_1, player_1_cumulative_sets_won, player_2_cumulative_sets_won) %>% replace_na(0)) %>% 
    ungroup() %>% 
    select(-starts_with("player_"),
           player_1,
           player_2,
           -standard_game_server_win,
           -standard_game_receiver_win,
           -tie_breaker_server_win,
           -tie_breaker_receiver_win) %>% 
    
    # now to determine whether set is match deciding or not (for either server or receiver)
    mutate(min_sets_for_straight_sets_win = if_else(gender == "men", 3, 2)) %>% 
    mutate(set_potential_status = case_when(
      (servers_cumulative_sets_won == (min_sets_for_straight_sets_win - 1) & receivers_cumulative_sets_won == 0) | 
        (servers_cumulative_sets_won >= min_sets_for_straight_sets_win & receivers_cumulative_sets_won <= (servers_cumulative_sets_won - 1)) ~ "potential match-deciding set for server",
      
      (receivers_cumulative_sets_won == (min_sets_for_straight_sets_win - 1) & servers_cumulative_sets_won == 0) | 
        (receivers_cumulative_sets_won >= min_sets_for_straight_sets_win & servers_cumulative_sets_won <= (receivers_cumulative_sets_won - 1)) ~ "potential match-deciding set for receiver",
      
      servers_cumulative_sets_won == (min_sets_for_straight_sets_win - 1) & receivers_cumulative_sets_won == (min_sets_for_straight_sets_win - 1) ~ "potential match-deciding set for both",
      
      TRUE ~ "not a potential match-deciding set")
    ) %>% 
    select(-min_sets_for_straight_sets_win)
  
  
  
  ###################################################################
  
  
  # source("R/make work required to win the rally function.R", local = T)
  # server_receiver_all_possible_score_combinations <- make_work_required_given_score_table()
  
  
  
  
  ad_advantage_score <- "AD"
  regular_0_to_40_scores <- c("0", "15", "30", "40")
  full_standard_game_scores <- c(regular_0_to_40_scores, ad_advantage_score)
  
  
  possible_standard_game_score_combinations <-
    crossing(server_score = full_standard_game_scores, receiver_score = regular_0_to_40_scores) %>% 
    bind_rows(
      crossing(server_score = regular_0_to_40_scores, receiver_score = full_standard_game_scores)
    ) %>% 
    distinct() %>% 
    mutate(
      server_score = factor(server_score, levels = full_standard_game_scores),
      receiver_score = factor(receiver_score, levels = full_standard_game_scores),
    ) %>% 
    filter(server_score %in% full_standard_game_scores) %>% 
    mutate(servers_distance_from_game_end = case_when(server_score == "AD" ~ 1,
                                                      server_score == "40" & receiver_score == "40" ~ 2,
                                                      TRUE ~ 5 - as.numeric(server_score)),
           receivers_distance_from_game_end = case_when(receiver_score == "AD" ~ 1,
                                                        server_score == "40" & receiver_score == "40" ~ 2,
                                                        TRUE ~ 5 - as.numeric(receiver_score)),
           servers_score_advantage_over_receiver = as.numeric(server_score) - as.numeric(receiver_score),
           score_is_in_standard_game_or_tie_breaker = "standard_game"
    )
  
  
  
  
  
  ###################################################################
  ###################################################################
  
  
  
  
  theoretical_max_tie_breaker_score <- 20
  possible_tie_breaker_scores <- 0:theoretical_max_tie_breaker_score
  
  
  combo_of_tie_breaker_score_vectors <- 
    crossing(server_score = possible_tie_breaker_scores, receiver_score = possible_tie_breaker_scores)
  
  
  
  ###################################################################
  
  non_win_by_two_standard_tie_breaker_scenarios <-
    combo_of_tie_breaker_score_vectors %>% 
    filter(server_score < min_score_to_win_in_standard_non_game_deciding_tie_breaker & receiver_score < min_score_to_win_in_standard_non_game_deciding_tie_breaker) %>% 
    mutate(servers_distance_from_game_end = min_score_to_win_in_standard_non_game_deciding_tie_breaker - server_score,
           receivers_distance_from_game_end = min_score_to_win_in_standard_non_game_deciding_tie_breaker - receiver_score,
           servers_score_advantage_over_receiver = server_score - receiver_score)
  
  
  win_by_two_standard_tie_breaker_scenarios <- 
    combo_of_tie_breaker_score_vectors %>% 
    filter(server_score >= min_score_to_win_in_standard_non_game_deciding_tie_breaker | receiver_score >= min_score_to_win_in_standard_non_game_deciding_tie_breaker) %>% 
    mutate(difference_between_scores = abs(server_score - receiver_score)) %>% 
    filter(difference_between_scores < 2) %>% 
    select(-difference_between_scores) %>% 
    mutate(servers_distance_from_game_end = case_when(
      server_score > receiver_score ~ 1,
      server_score == receiver_score ~ 2,
      server_score < receiver_score ~ 3),
      receivers_distance_from_game_end = case_when(
        receiver_score > server_score ~ 1,
        receiver_score == server_score ~ 2,
        receiver_score < server_score ~ 3),
      servers_score_advantage_over_receiver = server_score - receiver_score)
  
  
  
  possible_standard_tie_breaker_score_combinations <- 
    non_win_by_two_standard_tie_breaker_scenarios %>% 
    bind_rows(win_by_two_standard_tie_breaker_scenarios) %>% 
    mutate(score_is_in_standard_game_or_tie_breaker = "standard tie-breaker")
  
  
  
  
  ###################################################################
  
  # game deciding tie breaker
  
  
  non_win_by_two_game_deciding_tie_breaker_scenarios <-
    combo_of_tie_breaker_score_vectors %>% 
    filter(server_score < min_score_to_win_in_game_deciding_tie_breaker & receiver_score < min_score_to_win_in_game_deciding_tie_breaker) %>% 
    mutate(servers_distance_from_game_end = min_score_to_win_in_game_deciding_tie_breaker - server_score,
           receivers_distance_from_game_end = min_score_to_win_in_game_deciding_tie_breaker - receiver_score,
           servers_score_advantage_over_receiver = server_score - receiver_score)
  
  
  win_by_two_game_deciding_tie_breaker_scenarios <- 
    combo_of_tie_breaker_score_vectors %>% 
    filter(server_score >= min_score_to_win_in_game_deciding_tie_breaker | receiver_score >= min_score_to_win_in_game_deciding_tie_breaker) %>% 
    mutate(difference_between_scores = abs(server_score - receiver_score)) %>% 
    filter(difference_between_scores < 2) %>% 
    select(-difference_between_scores) %>% 
    mutate(servers_distance_from_game_end = case_when(
      server_score > receiver_score ~ 1,
      server_score == receiver_score ~ 2,
      server_score < receiver_score ~ 3),
      receivers_distance_from_game_end = case_when(
        receiver_score > server_score ~ 1,
        receiver_score == server_score ~ 2,
        receiver_score < server_score ~ 3),
      servers_score_advantage_over_receiver = server_score - receiver_score)
  
  
  
  possible_game_deciding_tie_breaker_score_combinations <- 
    non_win_by_two_game_deciding_tie_breaker_scenarios %>% 
    bind_rows(win_by_two_game_deciding_tie_breaker_scenarios) %>% 
    mutate(score_is_in_standard_game_or_tie_breaker = "match-deciding tie-breaker")
  
  
  
  ###################################################################
  
  
  possible_tie_breaker_score_combinations <- 
    possible_standard_tie_breaker_score_combinations %>% 
    bind_rows(possible_game_deciding_tie_breaker_score_combinations) %>% 
    mutate(across(c(server_score, receiver_score), ~.x %>% as.character()))
  
  
  
  
  ###################################################################
  
  
  server_receiver_all_possible_score_combinations <- 
    possible_standard_game_score_combinations %>% 
    bind_rows(possible_tie_breaker_score_combinations)
  
  
  
  game_related_features <- 
    score_related_features %>% 
    mutate(
      match_handedness_setup = case_when(
        receiver_handedness == "Right" & server_handedness == "Right" ~ "Same handedness-Both Right",
        receiver_handedness == "Left" & server_handedness == "Left" ~ "Same handedness-Both Left",
        receiver_handedness != server_handedness ~ "Different handedness"),
      
      server_to_receiver_handedness_setup = case_when(
        server_handedness == "Right" & receiver_handedness == "Right" ~ "Right to Right",
        server_handedness == "Right" & receiver_handedness == "Left" ~ "Right to Left",
        server_handedness == "Left" & receiver_handedness == "Left" ~ "Left to Left",
        server_handedness == "Left" & receiver_handedness == "Right" ~ "Left to Right"),
      
      court_side_and_server_to_receiver_handedness_setup_combo = paste0(court_side, "-", server_to_receiver_handedness_setup)
    ) %>% 
    mutate(
      additional_game_wins_required_until_set_win_for_server = if_else(
        (servers_total_cumulative_games_won_this_set == games_to_win_set & receivers_total_cumulative_games_won_this_set == games_to_win_set), 1,
        games_to_win_set - servers_total_cumulative_games_won_this_set), 
      current_game_win_advantage_for_server = servers_total_cumulative_games_won_this_set - receivers_total_cumulative_games_won_this_set) %>% 
    
    
    mutate(score_is_in_standard_game_or_tie_breaker = case_when(
      game_is_a_set_tie_breaker & set_potential_status %in% c(
        "potential match-deciding set for server", 
        "potential match-deciding set for receiver", 
        "potential match-deciding set for both"
      ) ~ "match-deciding tie-breaker",
      game_is_a_set_tie_breaker ~ "standard tie-breaker",
      TRUE ~ "standard_game")) %>% 
    
    left_join(server_receiver_all_possible_score_combinations, by = join_by(server_score, receiver_score, score_is_in_standard_game_or_tie_breaker)) %>% 
    select(-score_is_in_standard_game_or_tie_breaker)
  
  
  
  
  ###################################################################
  
  
  # there shouldn't be any NA values
  # it seems that sometimes
  # a tie-breaker is not being recognised as such
  
  
  replace_games_not_recognised_as_match_deciding <- function(game_related_features, server_receiver_all_possible_score_combinations) {
    
    server_receiver_all_possible_score_combinations %>% 
      filter(score_is_in_standard_game_or_tie_breaker == "match-deciding tie-breaker") %>% 
      select(server_score, receiver_score)
    
    
    games_not_recognised_as_match_deciding_tie_breakers <- 
      game_related_features %>% 
      filter(!complete.cases(select(., servers_distance_from_game_end, receivers_distance_from_game_end, servers_score_advantage_over_receiver))) %>% 
      semi_join(
        
        server_receiver_all_possible_score_combinations %>% 
          filter(score_is_in_standard_game_or_tie_breaker == "match-deciding tie-breaker") %>% 
          select(server_score, receiver_score)
        , by = join_by(server_score, receiver_score)
      ) %>% 
      distinct(match_id, game_number)
    
    
    these_rows_are_good_to_keep <- 
      game_related_features %>% 
      anti_join(games_not_recognised_as_match_deciding_tie_breakers, by = join_by(match_id, game_number))
    
    
    replacement_rows_for_those_games <- 
      game_related_features %>% 
      semi_join(games_not_recognised_as_match_deciding_tie_breakers, by = join_by(match_id, game_number)) %>% 
      select(-c(servers_distance_from_game_end, receivers_distance_from_game_end, servers_score_advantage_over_receiver)) %>% 
      mutate(score_is_in_standard_game_or_tie_breaker = "match-deciding tie-breaker") %>% 
      left_join(server_receiver_all_possible_score_combinations, by = join_by(server_score, receiver_score, score_is_in_standard_game_or_tie_breaker)) %>% 
      select(-score_is_in_standard_game_or_tie_breaker) %>% 
      filter(complete.cases(select(., servers_distance_from_game_end, receivers_distance_from_game_end, servers_score_advantage_over_receiver))) 
    
    
    end_result <- 
      these_rows_are_good_to_keep %>% 
      bind_rows(replacement_rows_for_those_games) %>% 
      arrange(CAX_ID)
    
    
    return(end_result)
    
  }
  
  
  
  
  
  with_game_heat_replacements <- 
    game_related_features %>% 
    replace_games_not_recognised_as_match_deciding(server_receiver_all_possible_score_combinations)
  
  
  ###################################################################
  
  # determine games required for the server to win the set
  
  
  part_2 <- 
    with_game_heat_replacements %>% 
    group_by(match_id, server_id) %>% 
    mutate(prev_serve_zone = lag(zone),
           prev_serve_was_fault = str_starts(lag(outcome), "fault"),
           prev_prev_serve_zone = lag(zone, 2),
           prev_prev_serve_was_fault = str_starts(lag(outcome, 2), "fault")) %>% 
    ungroup() %>% 
    mutate(prev_serve_type = if_else(prev_serve_was_fault, "fault", paste0("zone: ", prev_serve_zone)) %>% replace_na("no_data"),
           prev_prev_serve_type_combo = paste0(prev_serve_type, "|", paste0(if_else(prev_prev_serve_was_fault, "fault", paste0("zone: ", prev_prev_serve_zone)) %>% replace_na("no_data")))) %>% 
    select(-prev_serve_was_fault, -prev_serve_zone, -prev_prev_serve_was_fault, -prev_prev_serve_zone) %>% 
    group_by(match_id, server_id, court_side) %>%
    mutate(prev_serve_of_same_court_side_zone = lag(zone),
           prev_serve_of_same_court_side_was_fault = str_starts(lag(outcome), "fault"),
           prev_prev_serve_of_same_court_side_zone = lag(zone, 2),
           prev_prev_serve_of_same_court_side_was_fault = str_starts(lag(outcome, 2), "fault")) %>% 
    ungroup() %>% 
    mutate(prev_serve_of_same_court_side_type = if_else(prev_serve_of_same_court_side_was_fault, "fault", paste0("zone: ", prev_serve_of_same_court_side_zone)) %>% replace_na("no_data"),
           prev_prev_serve_type_combo = paste0(prev_serve_of_same_court_side_type, "|", paste0(if_else(prev_prev_serve_of_same_court_side_was_fault, "fault", paste0("zone: ", prev_prev_serve_of_same_court_side_zone)) %>% replace_na("no_data")))) %>% 
    select(-prev_serve_of_same_court_side_was_fault, -prev_serve_of_same_court_side_zone, -prev_prev_serve_of_same_court_side_was_fault, -prev_prev_serve_of_same_court_side_zone)
  
  
  
  
  part_3 <- 
    part_2 %>% 
    group_by(match_id) %>% 
    mutate(
      prev_set_number = lag(set_number),
      prev_game_number = lag(game_number),
      prev_point = lag(point),
      prev_outcome = lag(outcome),
      
      game_order_break_from_prev_row = case_when(
        set_number == 1 & game_number == 1 & point == 1 ~ "first point",
        is.na(prev_point) ~ "irregular blank but not first point", 
        set_number == prev_set_number & game_number == prev_game_number & point == prev_point + 1 ~ "good one",
        set_number == prev_set_number & game_number == prev_game_number & point == prev_point &  str_starts(prev_outcome, "fault") ~ "good one -prev was fault",
        set_number == prev_set_number & game_number == prev_game_number+1 & point == 1 ~ "good one -new game",
        set_number == prev_set_number+1 & game_number == 1 & point == 1 ~ "good one -new set") %>% 
        is.na(.)
    ) %>% 
    select(-c(prev_outcome, prev_point, prev_game_number, prev_set_number)) %>% 
    mutate(
      outcome_col_to_use = case_when(
        str_starts(outcome, "fault") & serve == 2 ~ "double fault", 
        str_starts(outcome, "fault") ~ "fault",
        outcome %in% c("winner", "forced_error", "unforced_error") & server_id == point_winner ~ paste0(outcome, "-server win"),
        outcome %in% c("winner", "forced_error", "unforced_error") & receiver_id == point_winner ~ paste0(outcome, "-receiver win"),
        TRUE ~ outcome
      )) %>% 
    group_by(match_id, server_id) %>% 
    mutate(prev_rally_outcome = lag(outcome_col_to_use) %>% replace_na("no_data"),
           prev_prev_rally_outcome_combo = paste0(prev_rally_outcome, "|", lag(outcome_col_to_use, 2) %>% replace_na("no_data"))) %>% 
    group_by(match_id, server_id, court_side) %>%
    mutate(prev_rally_outcome_same_court_side = lag(outcome_col_to_use) %>% replace_na("no_data"),
           prev_prev_rally_outcome_same_court_side_combo= paste0(prev_rally_outcome_same_court_side, "|", lag(outcome_col_to_use, 2) %>% replace_na("no_data"))) %>% 
    ungroup() %>% 
    mutate(is_player_1_ace = if_else(outcome_col_to_use == "ace" & point_winner == player_1, 1, NA_real_),
           is_player_2_ace = if_else(outcome_col_to_use == "ace" & point_winner == player_2, 1, NA_real_),
           
           is_player_1_fault = if_else(outcome_col_to_use == "fault" & point_winner == player_1, 1, NA_real_),
           is_player_2_fault = if_else(outcome_col_to_use == "fault" & point_winner == player_2, 1, NA_real_),
           
           is_player_1_double_fault = if_else(outcome_col_to_use == "double fault" & point_winner == player_1, 1, NA_real_),
           is_player_2_double_fault = if_else(outcome_col_to_use == "double fault" & point_winner == player_2, 1, NA_real_),
           
           is_player_1_win = if_else(point_winner == player_1 & outcome_col_to_use != "fault",  1, NA_real_),
           is_player_2_win = if_else(point_winner == player_2 & outcome_col_to_use != "fault",  1, NA_real_),
           
           is_player_1_loss = if_else(is_player_2_win == 1,  1, NA_real_),
           is_player_2_loss = if_else(is_player_1_win == 1,  1, NA_real_),
           
    ) %>% 
    select(-outcome, -outcome_col_to_use) %>% 
    
    group_by(match_id) %>% 
    mutate(
      servers_total_aces_so_far = if_else(server_id == player_1, lag(is_player_1_ace %>% replace_na(0) %>% cumsum()), lag(is_player_2_ace %>% replace_na(0) %>% cumsum())) %>% replace_na(0),
      receivers_total_aces_so_far = if_else(receiver_id == player_1, lag(is_player_1_ace %>% replace_na(0) %>% cumsum()), lag(is_player_2_ace %>% replace_na(0) %>% cumsum())) %>% replace_na(0),
      
      servers_total_double_faults_so_far = if_else(server_id == player_1, lag(is_player_1_double_fault %>% replace_na(0) %>% cumsum()), lag(is_player_2_double_fault %>% replace_na(0) %>% cumsum())) %>% replace_na(0),
      receivers_total_double_faults_so_far = if_else(receiver_id == player_1, lag(is_player_1_double_fault %>% replace_na(0) %>% cumsum()), lag(is_player_2_double_fault %>% replace_na(0) %>% cumsum())) %>% replace_na(0),
      
      servers_total_faults_so_far = if_else(server_id == player_1, lag(is_player_1_fault %>% replace_na(0) %>% cumsum()), lag(is_player_2_fault %>% replace_na(0) %>% cumsum())) %>% replace_na(0),
      receivers_total_faults_so_far = if_else(receiver_id == player_1, lag(is_player_1_fault %>% replace_na(0) %>% cumsum()), lag(is_player_2_fault %>% replace_na(0) %>% cumsum())) %>% replace_na(0),
      
      server_won_previous_point = if_else(server_id == player_1, lag(is_player_1_win) == 1, lag(is_player_2_win) == 1) %>% replace_na(FALSE),
      receiver_won_previous_point = if_else(receiver_id == player_1, lag(is_player_1_win) == 1, lag(is_player_2_win) == 1) %>% replace_na(FALSE),
      
      server_lost_previous_point = if_else(server_id == player_1, lag(is_player_1_loss) == 1, lag(is_player_2_loss) == 1) %>% replace_na(FALSE),
      receiver_lost_previous_point = if_else(receiver_id == player_1, lag(is_player_1_loss) == 1, lag(is_player_2_loss) == 1) %>% replace_na(FALSE),
      
    ) %>% 
    group_by(match_id, set_number, game_number) %>% 
    mutate(
      servers_total_aces_so_far_in_current_game = if_else(server_id == player_1, lag(is_player_1_ace %>% replace_na(0) %>% cumsum()), lag(is_player_2_ace %>% replace_na(0) %>% cumsum())) %>% replace_na(0),
      receivers_total_aces_so_far_in_current_game = if_else(receiver_id == player_1, lag(is_player_1_ace %>% replace_na(0) %>% cumsum()), lag(is_player_2_ace %>% replace_na(0) %>% cumsum())) %>% replace_na(0),
      
      servers_total_double_faults_so_far_in_current_game = if_else(server_id == player_1, lag(is_player_1_double_fault %>% replace_na(0) %>% cumsum()), lag(is_player_2_double_fault %>% replace_na(0) %>% cumsum())) %>% replace_na(0),
      receivers_total_double_faults_so_far_in_current_game = if_else(receiver_id == player_1, lag(is_player_1_double_fault %>% replace_na(0) %>% cumsum()), lag(is_player_2_double_fault %>% replace_na(0) %>% cumsum())) %>% replace_na(0),
      
      servers_total_faults_so_far_in_current_game = if_else(server_id == player_1, lag(is_player_1_fault %>% replace_na(0) %>% cumsum()), lag(is_player_2_fault %>% replace_na(0) %>% cumsum())) %>% replace_na(0),
      receivers_total_faults_so_far_in_current_game = if_else(receiver_id == player_1, lag(is_player_1_fault %>% replace_na(0) %>% cumsum()), lag(is_player_2_fault %>% replace_na(0) %>% cumsum())) %>% replace_na(0),
      
      server_won_previous_point_in_current_game = if_else(server_id == player_1, lag(is_player_1_win) == 1, lag(is_player_2_win) == 1) %>% replace_na(FALSE),
      receiver_won_previous_point_in_current_game = if_else(receiver_id == player_1, lag(is_player_1_win) == 1, lag(is_player_2_win) == 1) %>% replace_na(FALSE),
      
      server_lost_previous_point_in_current_game = if_else(server_id == player_1, lag(is_player_1_loss) == 1, lag(is_player_2_loss) == 1) %>% replace_na(FALSE),
      receiver_lost_previous_point_in_current_game = if_else(receiver_id == player_1, lag(is_player_1_loss) == 1, lag(is_player_2_loss) == 1) %>% replace_na(FALSE),
      
    ) %>% 
    ungroup() %>% 
    select(-starts_with("is_player_"))
  
  
  
  
  return(part_3)
  
  
}



###################################################################

# let's use the above function to generate the features for all rallies in the dataset

tictoc::tic()

train_test_combination_with_extra_info_cols <- 
  train_and_test_appended %>% 
  make_the_features()

tictoc::toc()

# 165.85 sec elapsed


###################################################################

# we now need to make the xgboost model


# first we isolate the features we have previously investigated to be helpful

important_variables <- 
  c("serve", "gender", "court_side", "receiver_position_x", "point", 
    "court_side_and_server_to_receiver_handedness_setup_combo", "prev_rally_outcome_same_court_side", 
    "prev_rally_outcome", "servers_total_aces_so_far", "server_won_previous_point", 
    "receiver_won_previous_point", "server_won_previous_point_in_current_game", 
    "receiver_lost_previous_point_in_current_game", "server_lost_previous_point", 
    "receiver_lost_previous_point", "servers_score_advantage_over_receiver", 
    "receivers_total_faults_so_far", "servers_total_faults_so_far", 
    "receivers_total_double_faults_so_far", "receivers_total_aces_so_far", 
    "receivers_cumulative_games_won_as_server", "servers_cumulative_games_won_as_server", 
    "receiver_won_previous_point_in_current_game", "receivers_total_cumulative_games_won", 
    "servers_total_cumulative_games_won", "receivers_cumulative_games_won_as_receiver", 
    "servers_distance_from_game_end"
    
    , "match_number_for_server"
    , "match_number_for_receiver"
    , "reciever_court_side"
    
  )



###################################################################

# we can now separate the training data that we are using for the xgboost model
# we are using the rallies that preceed the first test point for training our xgboost model


training_data <- 
  train_test_combination_with_extra_info_cols %>% 
  isolate_training_data(specific_test_row_index = 1, {{rows_to_use_for_testing}}) %>% 
  select(zone, all_of(important_variables))



# we need to recreate our test point rows, from the full set of data (with all features)

all_test_rows <- 
  train_test_combination_with_extra_info_cols %>% 
  mutate(test_row_index_to_use = {{rows_to_use_for_testing}}, .before = 1) %>% 
  filter(!is.na(test_row_index_to_use)) %>% 
  select(-any_of(c("test_row_index", "pseudo_test_row_index", "from_training_data_file", "is_test_row", "is_pseudo_test_row"))) %>% 
  mutate(zone = factor(zone)) 


###################################################################


# this was the best for tuning an xgboost model handling all rows preceding the first test row
# we used the "pseudo test rows" to determine this
# the hyper parameter tuning process is not included in this script


xgboost_config_with_best_log_loss <-
  tibble::tribble(
    ~mtry, ~trees, ~min_n, ~tree_depth, ~learn_rate, ~loss_reduction, ~sample_size,
    25L,  1661L,    25L,          6L, 0.004096375,         2.5e-07,   0.57890875
  )




# this function deploys a tidymodels process to train an xgboost model


train_a_model <- function(data, hyper_param_config) {
  
  
  # data <- training_data
  # hyper_param_config <- xgboost_config_with_best_log_loss
  
  
  data_import_prep <- 
    data %>% 
    filter(complete.cases(select(., -receiver_position_x)))
  
  
  recipe_to_use <- 
    recipe(zone ~ ., data = data_import_prep) %>% 
    step_integer(all_logical_predictors()) %>% 
    step_novel(all_nominal_predictors()) %>%
    step_unknown(all_nominal_predictors()) %>%
    step_dummy(all_nominal_predictors(), one_hot = FALSE) %>% 
    step_impute_median(all_numeric_predictors())
  
  
  
  
  model_spec <-
    boost_tree(
      trees = tune()
      , tree_depth = tune()
      , min_n = tune()
      , loss_reduction = tune()
      , sample_size = tune()
      , mtry = tune()
      , learn_rate = tune()
      , stop_iter = 200
    ) %>%
    set_engine("xgboost"
               , validation = 0.2
               , nthread = parallelly::availableCores()) %>%
    set_mode("classification")
  
  
  
  
  finalised_model <- 
    model_spec %>% 
    finalize_model(hyper_param_config)
  
  
  
  finalised_workflow <- 
    workflow() %>% 
    add_recipe(recipe_to_use) %>% 
    add_model(finalised_model)
  
  
  
  fitted_model <- 
    fit(finalised_workflow, data_import_prep) %>% 
    butcher::butcher()
  
  
  return(fitted_model)
  
  
}




###################################################################

# lets do twp runs of the xgboost trainign and predicting 
# and take the average... so that any anomalous predicted values might be "smoothed out"

tictoc::tic()

set.seed(123)

collection_of_results <- c()

for (i in 1:2) {
  
  trained_model <- train_a_model(training_data, hyper_param_config = xgboost_config_with_best_log_loss)
  
  xgboost_model_from_first_test_row_only <- 
    all_test_rows %>%  
    bind_cols(
      predict(trained_model, new_data = all_test_rows, type = "prob") %>% set_names(as.character(0:5))
    ) %>% 
    select(test_row_index_to_use, zone, `0`:`5`)
  
  collection_of_results <- 
    bind_rows(collection_of_results, xgboost_model_from_first_test_row_only) %>% 
    mutate(iteration = i)
  
  print(i)
  
}


tictoc::toc()

# 353.87 sec elapsed


xgboost_model_from_first_test_row_only <- 
  collection_of_results %>% 
  select(-iteration) %>% 
  gather(key, value, -test_row_index_to_use, -zone) %>% 
  group_by(test_row_index_to_use, zone, key) %>% 
  summarise(average_value = mean(value)) %>% 
  spread(key, average_value) %>% 
  ungroup()



###################################################################


if (FALSE) {
  
  xgboost_model_from_first_test_row_only %>% 
    yardstick::mn_log_loss(zone, `0`:`5`) %>% 
    pluck(".estimate")
  
  # [1] 1.585664
  
  ###################################################################
  
  models_top_variables <- 
    trained_model %>%
    extract_fit_engine() %>% 
    vi() %>% 
    as_tibble() %>% 
    mutate(Importance = round(Importance, 4))
  
  
  models_top_variables %>%
    mutate(row_id = row_number(),
           cumulative_importance_percent = cumsum(Importance)/1) %>%
    ggplot() +
    aes(row_id, cumulative_importance_percent) +
    geom_line(stat = "identity") +
    scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
    expand_limits(y = 0)
  
  
}


###################################################################
###################################################################


combination_of_methods <- 
  all_test_rows %>% 
  select(test_row_index_to_use, zone, CAX_ID, match_id) %>% 
  left_join(
    
    combination_of_rates_by_server %>% 
      select(test_row_index_to_use, `0`:`5`) %>% 
      nest(combination_of_rates_by_server := -test_row_index_to_use)
    
    , by = join_by(test_row_index_to_use)) %>% 
  left_join(
    
    xgboost_model_from_first_test_row_only %>% 
      select(test_row_index_to_use,  `0`:`5`) %>% 
      nest(xgboost_model_from_first_test_row_only := -test_row_index_to_use)
    
    , by = join_by(test_row_index_to_use)) %>% 
  gather(method, predictions, -c(test_row_index_to_use, zone,  CAX_ID, match_id)) %>% 
  unnest(predictions, keep_empty = T)




best_method_weighting_combo <-
  tibble::tribble(
    ~method, ~weighting,
    "combination_of_rates_by_server", 3L,
    "xgboost_model_from_first_test_row_only", 1L,
    
  )



combined_model_predictions <- 
  best_method_weighting_combo %>% 
  mutate(method_with_weight = paste0("(", weighting, "x)", method)) %>% 
  uncount(weighting) %>% 
  inner_join(combination_of_methods, by = "method", multiple = "all") %>% 
  gather(zone_for_pred, value, `0`:`5`) %>% 
  group_by(test_row_index_to_use, CAX_ID, match_id, zone, zone_for_pred) %>%
  summarise(
    method_combo = paste0(method_with_weight, collapse = "|"),
    prediction_value = mean(value, na.rm = T)
  ) %>% 
  group_by(test_row_index_to_use, CAX_ID, match_id, zone) %>%
  mutate(prediction_value = prediction_value/sum(prediction_value)) %>% 
  ungroup() %>% 
  spread(zone_for_pred, prediction_value, fill = 0) %>% 
  select(-method_combo)




if (FALSE) {
  
  combined_model_predictions %>% 
    yardstick::mn_log_loss(zone, `0`:`5`) %>% 
    pluck(".estimate")
  
  
  # [1] 1.561371
  
}





combined_model_predictions %>% 
  select(CAX_ID, match_id, `0`:`5`) %>% 
  write_csv("submission_file.csv")




