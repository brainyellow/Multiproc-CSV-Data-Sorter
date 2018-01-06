#ifndef SORTER_H
#define SORTER_H

//trim function prototype
char* trim(char *str);

//Suggestion: define a struct that mirrors a record (row) of the data set
struct Movie
{
  char* color;
  char* director_name;
  char* num_critic_for_reviews;
  char* duration;
  char* director_facebook_likes;
  char* actor_3_facebook_likes;
  char* actor_2_name;
  char* actor_1_facebook_likes;
  char* gross;
  char* genres;
  char* actor_1_name;
  char* movie_title;
  char* num_voted_users;
  char* cast_total_facebook_likes;
  char* actor_3_name;
  char* facenumber_in_poster;
  char* plot_keywords;
  char* movie_imdb_link;
  char* num_user_for_reviews;
  char* language;
  char* country;
  char* content_rating;
  char* budget;
  char* title_year;
  char* actor_2_facebook_likes;
  char* imdb_score;
  char* aspect_ratio;
  char* movie_facebook_likes;  
};
//mergesort prototype
void mergesort(int selection, struct Movie* arr, int low, int high);

#endif
