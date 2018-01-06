#ifndef SORTER_H
#include "sorter.h"
#endif

void merge(int selection, struct Movie* arr, int low, int mid, int high);
int moviecmp(int selection, struct Movie first, struct Movie second);
int numcmp(char* first, char* second);
int fltcmp(char* first, char* second);

void mergesort(int selection, struct Movie* arr, int low, int high)
{
	
	if(low < high)
	{
		int mid = (low + high) / 2;
		
		//split the array
		mergesort(selection, arr, low, mid);
		mergesort(selection, arr, mid + 1, high);
		
		merge(selection, arr, low, mid, high);
	}
}

void merge(int selection, struct Movie* arr, int low, int mid, int high)
{
	int i, j, k;
	int n1 = mid - low + 1;
	int n2 = high - mid;
	
	//temp movie arrays
	struct Movie* arrtl = malloc(sizeof(struct Movie) *n1);
	struct Movie* arrtr = malloc(sizeof(struct Movie) *n2);

	//copy data into temps
	for(i = 0; i < n1; i++)
		arrtl[i] = arr[low + i];
	for(j = 0; j < n2; j++)
		arrtr[j] = arr[mid + 1 + j];

	i = 0;
	j = 0;
	k = low;

	while(i < n1 && j < n2)
	{	
		//Compare the values
		if(moviecmp(selection, arrtl[i], arrtr[j]) <= 0)
		{
			arr[k] = arrtl[i];
			i++;
		}
		else
		{
			arr[k] = arrtr[j];
			j++;
		}
		k++;
	}
	
	while(i < n1)
	{
		arr[k] = arrtl[i];
		i++;
		k++;
	}

	while(j < n2)
	{
		arr[k] = arrtr[j];
		j++;
		k++;
	}
	free(arrtr);
	free(arrtl);
}

/*
 * Return int > 0 if first value is greater than second
 * Return int < 0 if first value is less than second
 * Return int = 0 if values are equla
 */

int moviecmp(int selection, struct Movie first, struct Movie second)
{
	//Compare the movie attribute defined by int selection
	if(selection == 0)
		return strcmp(first.color, second.color);
	if(selection == 1)
		return strcmp(first.director_name, second.director_name);
	if(selection == 2)
		return numcmp(first.num_critic_for_reviews,second.num_critic_for_reviews);
	if(selection == 3)
		return numcmp(first.duration,second.duration);
	if(selection == 4)
		return numcmp(first.director_facebook_likes,second.director_facebook_likes);
	if(selection == 5)
		return numcmp(first.actor_3_facebook_likes,second.actor_3_facebook_likes);
	if(selection == 6)
		return strcmp(first.actor_2_name, second.actor_2_name);
	if(selection == 7)
		return numcmp(first.actor_1_facebook_likes,second.actor_1_facebook_likes);
	if(selection == 8)
		return numcmp(first.gross,second.gross);
	if(selection == 9)
		return strcmp(first.genres, second.genres);
	if(selection == 10)
		return strcmp(first.actor_1_name, second.actor_1_name);
	if(selection == 11)
		return strcmp(first.movie_title, second.movie_title);
	if(selection == 12)
		return numcmp(first.num_voted_users,second.num_voted_users);
	if(selection == 13)
		return numcmp(first.cast_total_facebook_likes,second.cast_total_facebook_likes);
	if(selection == 14)
		return strcmp(first.actor_3_name, second.actor_3_name);
	if(selection == 15)
		return numcmp(first.facenumber_in_poster,second.facenumber_in_poster);
	if(selection == 16)
		return strcmp(first.plot_keywords, second.plot_keywords);
	if(selection == 17)
		return strcmp(first.movie_imdb_link, second.movie_imdb_link);
	if(selection == 18)
		return numcmp(first.num_user_for_reviews,second.num_user_for_reviews);
	if(selection == 19)
		return strcmp(first.language, second.language);
	if(selection == 20)
		return strcmp(first.country, second.country);
	if(selection == 21)
		return strcmp(first.content_rating, second.content_rating);
	if(selection == 22)
		return numcmp(first.budget,second.budget);
	if(selection == 23)
		return numcmp(first.title_year,second.title_year);
	if(selection == 24)
		return numcmp(first.actor_2_facebook_likes,second.actor_2_facebook_likes);
	if(selection == 25)
		return fltcmp(first.imdb_score, second.imdb_score);
	if(selection == 26)
		return fltcmp(first.aspect_ratio, second.aspect_ratio);
	if(selection == 27)
		return numcmp(first.movie_facebook_likes,second.movie_facebook_likes);
	
	return 0;
}
//function for comparing numbers, not floats
int numcmp(char* first, char* second)
{
	if(atol(first) != atol(second))
	{
		if (atol(first) > atol(second))
		{
			return 1;
		}
		else
			return -1;
	}
	else
		return 0;
}
//function for comparing floats
int fltcmp(char* first, char* second)
{
	if(atof(first) != atof(second))
	{
		if(atof(first) > atof(second))
		{
			return 1;
		}
		else
			return -1;
	}
	else
		return 0;
}



