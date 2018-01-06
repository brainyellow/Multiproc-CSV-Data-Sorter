#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <dirent.h>
#include <sys/mman.h>
#include <stdbool.h>
#include "sorter.h"
#include "mergesort.c"

int main(int argc, char * argv[])
{

	//Checking if argv[1] has an input
	if (argv[1] == 0)
	{
		printf("-c not entered\n");
		return -1;
	}
	//Checking if argv[1] is -c
	if (strcmp(argv[1],"-c") != 0)
	{
		printf("Did not enter -c\n");
		return -1;
	}
	//Checking if argv[2] has an input
	if (argv[2] == 0)
	{
		printf("Did not enter header column input\n");
		return -1;
	}
	
	char inputdir[1024];
	char outputdir[1024];
	char tempwd[1024];
	char* tempoutputdir = malloc(1000);
	bool outputgiven  = false;

	if (argc > 3)
	{
		if (strcmp(argv[3],"-d") == 0)
		{
			if (argc > 5)
			{
				if (strcmp(argv[5],"-o") != 0)
				{
					printf("Incorrect parameters\n");
					return -1;
				}
				else if(argc < 7)
				{
					printf("No argument entered after -o\n");
					return -1;
				}
				else
				{
					//input directory is given directory
					strcpy(inputdir, argv[4]);
					//output directory is given directory
					outputgiven = true;
					strcpy(outputdir, argv[6]);

					if((outputdir[0] == '.') && (outputdir[1] == '/'))
					{
						getcwd(tempwd, sizeof(tempwd));
						strcpy(tempoutputdir, outputdir);
						//getting rid of dot using strtok
						strcpy(tempoutputdir,strtok(tempoutputdir, "."));
						strcat(tempwd, tempoutputdir);
						strcpy(outputdir, tempwd);
					} 
					else if(outputdir[0] != '/')
					{
						getcwd(tempwd, sizeof(tempwd));
						strcat(tempwd, "/");
						strcat(tempwd, outputdir);
						strcpy(outputdir, tempwd);
					}
				}
			}
			else if (argc < 5)
			{
				printf("No argument entered after -d\n");
				return -1;
			}
			else
			{
				//input directory is given directory
				strcpy(inputdir, argv[4]);
				//output directory is source file directory
			}
		}
		else if (strcmp(argv[3],"-o") == 0)
		{
			if (argc < 5)
			{
				printf("No argument entered after -o\n");
				return -1;
			}
			if (argc > 5)
			{
				printf("Incorrect parameters\n");
				return -1;
			}

			//input directory is current directory
			getcwd(inputdir, sizeof(inputdir));
			//output directory is given directory
			outputgiven = true;
			strcpy(outputdir, argv[4]);

			if((outputdir[0] == '.') && (outputdir[1] == '/'))
			{
				getcwd(tempwd, sizeof(tempwd));
				strcpy(tempoutputdir, outputdir);
				//getting rid of dot using strtok
				strcpy(tempoutputdir,strtok(tempoutputdir, "."));
				strcat(tempwd, tempoutputdir);
				strcpy(outputdir, tempwd);
			} 
			else if(outputdir[0] != '/')
			{
				getcwd(tempwd, sizeof(tempwd));
				strcat(tempwd, "/");
				strcat(tempwd, outputdir);
				strcpy(outputdir, tempwd);
			}
		}
		else 
		{
			printf("Incorrect parameters\n");
			return -1;
		}
	}
	else
	{
		//input directory is current directory
		getcwd(inputdir, sizeof(inputdir));
		//output directory is source file directory
	}
	//free pointer
	free(tempoutputdir);

	int initialpid = getpid();	
	int pid;
	int status;
	int processcount = 0;
	int entrycounter = 0;
	bool isnewdirproc = false;
	bool isfirstchild = true;
	struct dirent *nextentry;
	DIR *dir;
	DIR *ndir;
	char nextdir[1024];
	char tempdir[1024];
	char d_name[1024];

	printf("Initial PID: %d\n", initialpid);
	printf("PIDS of all child processes: ");
	fflush(stdout);

	if ((dir = opendir(inputdir)) == NULL)
	{
		printf("Can't open %s\n", inputdir);
		return -1;
	}

	strcpy(nextdir, inputdir);

	while((nextentry = readdir(dir)) != NULL)
	{
		//if entry is a csv file then fork
		//if entry is a directory then fork
		//if entry is neither then do nothing and continue
		
		strcpy(d_name, nextentry->d_name);
		entrycounter++;

		if(entrycounter > 2)
		{
			strcpy(tempdir, nextdir);
			strcat(nextdir, "/");
			strcat(nextdir, d_name);

			if ((ndir = opendir(nextdir)) != NULL)
			{	
				//current entry is a directory
				pid = fork();

				if (pid == 0) {
					//child
					isfirstchild = false;
					dir = ndir;
					strcpy(inputdir, nextdir);
					entrycounter = 0;
					processcount = 1;
					isnewdirproc = true;
				} else if (pid == -1) {
					printf("Error in fork");
					return -1;
				} else {
					// parent and the child is running	
					if(isfirstchild)
						printf("%d", pid);
					else
						printf(", %d", pid);
					fflush(stdout);
					isfirstchild = false;
					waitpid(pid, &status, 0);
					processcount += WEXITSTATUS(status);
					strcpy(nextdir, tempdir);
				}
			}
			else
			{
				strcpy(nextdir, tempdir);
			} 
		}
		
		char *dot = strrchr(d_name, '.');
		if (dot && !strcmp(dot, ".csv") && (strstr(d_name, "-sorted") == NULL))
		{
			pid = fork();
			if (pid == 0) {
				// child
				// break out of loop to go sort
				isnewdirproc = false;
				break;
			} else if (pid == -1) {
				printf("Error in fork");
				return -1;
			} else {
				// parent and the child is running.
				// continue with loop, increment processcount, and print latest child pid
				if(isfirstchild)
					printf("%d", pid);
				else
					printf(", %d", pid);
				fflush(stdout);
				isfirstchild = false;
				processcount++;
			}
		}
	}

	if(isnewdirproc)
		exit(processcount);

	if (pid == 0) {
		// child
		// sort file that is d_name
		
		//size of CSV
		size_t sizeCSV = 1000;
		char* CSV = malloc(sizeof(char)*sizeCSV);
		
		//stores size of trimBuffer
		size_t sizeTrimBuffer = 50;
		char* trimBuffer = malloc(sizeof(char)*sizeTrimBuffer);
		
		//tokens
		char* tempStr;	
		char* fronttoken;
		char* token;
		char* backtoken;
		char* title;
		
		//size of initial malloc for Movie array
		size_t sizeMovies = 1000;
		
		//allocate mem for array of movie structs
		struct Movie* movies = malloc(sizeof(struct Movie)*sizeMovies);

		//compare each token to user input (argv[2]) with headerFlag
		int headerFlag = 0;
		
		//counter and variable to keep track of which column to sort by
		int sortColumnCounter = 0;
		int columnToSort = 0;

		//changing working directory for each child process
		chdir(inputdir);
		//creating file stream to read for CSV to be sorted
		FILE* CSVStream;
		CSVStream = fopen(d_name, "r");
		if (CSVStream == NULL)
		{
			exit(1);
		}
		
		//get first line (headers)
		fgets(CSV, sizeof(char)*sizeCSV,CSVStream);
		
		char* tempHeader = strdup(CSV);
		//making sure the format of the CSV is correct (28 columns)	
		token = strtok(tempHeader, ",");
		while(token != NULL)
		{
			//compare header tokens with user defined sort parameter
			if(strcmp(token, argv[2])== 0)
			{
				headerFlag = 1;
				columnToSort = sortColumnCounter;
			}
			sortColumnCounter++;
			token = strtok(NULL, ",");
		}

		//check if what we're sorting has correct headers
		if(headerFlag == 0)
		{
			//stop this process from sorting because it does not contain the correct headers
			exit(1);
		}
		if(sortColumnCounter != 28)
		{
			//stop process if # columns incorrect
			exit(1);
		}

		if(!outputgiven)
			strcpy(outputdir,inputdir);
		else if(opendir(outputdir) == NULL)
			mkdir(outputdir, S_IRWXU);
		
		//change dir to output directory
		chdir(outputdir);
		//creating new sorted file name
		char* sortedName;
		sortedName = strtok(d_name, ".");
		strcat(sortedName, "-sorted-");
		strcat(sortedName, argv[2]);
		strcat(sortedName, ".csv");
		FILE* CSVSorted;
	
		CSVSorted = fopen(sortedName, "w");

		//resetting counter
		sortColumnCounter = 0;

		token = strtok(CSV, ",");
		while(token != NULL)
		{	
			//printing column headers
			if (sortColumnCounter < 27)
				fprintf(CSVSorted,"%s,", token);
			if (sortColumnCounter == 27)
				fprintf(CSVSorted,"%s", token);

			token = strtok(NULL, ",");
			sortColumnCounter++;
		}
	
		//counter for current struct in movies[]
		int i = 0;	
		//counter for current location in individual struct
		int n = 0;

		while(fgets(CSV, sizeof(char)*sizeCSV, CSVStream))
		{	
			//reallocs when i counter is larger than size of initial movies malloc
			if (i > sizeMovies)
			{
				//add 1000 movies to size of movies array
				sizeMovies += 1000;		//adding 1000 movies to size of movies array
				movies = realloc(movies, sizeof(struct Movie)*sizeMovies);
				
				//exception if realloc returns NULL
				if (movies == NULL)
				{
					printf("Realloc failed, not enough memory\n");
					return -1; 			
				}
			}

			//dups CSV line
			tempStr = strdup(CSV);
			//stores line until first quote
			fronttoken = strsep(&tempStr, "\"");
			//stores line from first quote > second quote
			title = strsep(&tempStr, "\"");
			
			//stores line from second quote > newline
			backtoken = strsep(&tempStr, "\n");

			/*
			* strdup seg faults if strdup dups a NULL
			* tokenize fronttoken, if not NULL condition for examples where fronttoken is NULL, 
			* such the first element having quotes (robust code)
			*/
			if(fronttoken != NULL)
			{
				tempStr = strdup(fronttoken);
				//reallocing trimBuffer based on size of fronttoken. Since fronttoken is sometimes the whole line, it will be largest possible string to store
				while(strlen(tempStr) > sizeTrimBuffer)		
				{	//doubling size until larger than fronttoken
					sizeTrimBuffer += 50;
					trimBuffer = realloc(trimBuffer, sizeof(char)*sizeTrimBuffer);
					//if not enough mem
					if(trimBuffer == NULL)
					{
						printf("Not enough memory\n");
						return -1;
					}
				}
				/*
				* trimming leading and trailing whitespace for fronttoken because if there is leading whitespace in front of title,
				* fronttoken will have trailing whitespace, which causes extra comma to persist
				*/
				strcpy(trimBuffer, tempStr);									
				strcpy(tempStr, trim(trimBuffer));

				//deleting extra comma to avoid creating extra token
				if (title != NULL)
				{
					tempStr[strlen(tempStr) - 1] = 0;
				}
				
				//tokenize fronttoken using comma delim
				token = strsep(&tempStr, ",");
				while(token != NULL)
				{
					//trimming whitespace in token
					strcpy(trimBuffer, token);
					strcpy(token, trim(trimBuffer));
					//if statements to place current token in correct struct variable
					//Includes all variables
					if(n == 0)
						movies[i].color = token;
					if(n == 1)
						movies[i].director_name = token;
					if(n == 2)
						movies[i].num_critic_for_reviews = token;
					if(n == 3)
						movies[i].duration = token;
					if(n == 4)
						movies[i].director_facebook_likes = token;
					if(n == 5)
						movies[i].actor_3_facebook_likes = token;
					if(n == 6)
						movies[i].actor_2_name = token;
					if(n == 7)
						movies[i].actor_1_facebook_likes = token;
					if(n == 8)
						movies[i].gross = token;
					if(n == 9)
						movies[i].genres = token;
					if(n == 10)
						movies[i].actor_1_name = token;
					if(n == 11)
						movies[i].movie_title = token;
					if(n == 12)
						movies[i].num_voted_users = token;
					if(n == 13)
						movies[i].cast_total_facebook_likes = token;
					if(n == 14)
						movies[i].actor_3_name = token;
					if(n == 15)
						movies[i].facenumber_in_poster = token;
					if(n == 16)
						movies[i].plot_keywords = token;
					if(n == 17)
						movies[i].movie_imdb_link = token;
					if(n == 18)
						movies[i].num_user_for_reviews = token;
					if(n == 19)
						movies[i].language = token;
					if(n == 20)
						movies[i].country = token;
					if(n == 21)
						movies[i].content_rating = token;
					if(n == 22)
						movies[i].budget = token;
					if(n == 23)
						movies[i].title_year = token;
					if(n == 24)
						movies[i].actor_2_facebook_likes = token;
					if(n == 25)
						movies[i].imdb_score = token;
					if(n == 26)
						movies[i].aspect_ratio = token;
					if(n == 27)
						movies[i].movie_facebook_likes = token;
					token = strsep(&tempStr, ",");
					n++;
				}
			}
			//Only tokenize backtoken if NOT NULL
			if(backtoken != NULL)
			{
				tempStr = strdup(backtoken);
				if(title != NULL)
				{
					strcpy(trimBuffer, title);
					strcpy(title, trim(trimBuffer));
					//if statements to place current token in correct struct variable
					//Only including title variable
					if(n == 11)
						movies[i].movie_title = title;
					
					token = strsep(&tempStr,",");	//Get rid of extra comma meant for title containing title
					n++;
				}

				token = strsep(&tempStr, ",");
				while(token != NULL)
				{
					//trimming whitespace
					strcpy(trimBuffer, token);
					strcpy(token, trim(trimBuffer));
					//if statements to place current token in correct struct variable
					//Only includes variables after movie title	
					if(n == 12)
						movies[i].num_voted_users = token;
					if(n == 13)
						movies[i].cast_total_facebook_likes = token;
					if(n == 14)
						movies[i].actor_3_name = token;
					if(n == 15)
						movies[i].facenumber_in_poster = token;
					if(n == 16)
						movies[i].plot_keywords = token;
					if(n == 17)
						movies[i].movie_imdb_link = token;
					if(n == 18)
						movies[i].num_user_for_reviews = token;
					if(n == 19)
						movies[i].language = token;
					if(n == 20)
						movies[i].country = token;
					if(n == 21)
						movies[i].content_rating = token;
					if(n == 22)
						movies[i].budget = token;
					if(n == 23)
						movies[i].title_year = token;
					if(n == 24)
						movies[i].actor_2_facebook_likes = token;
					if(n == 25)
						movies[i].imdb_score = token;
					if(n == 26)
						movies[i].aspect_ratio = token;
					if(n == 27)
						movies[i].movie_facebook_likes = token;
					
					token = strsep(&tempStr, ",");
					n++;
				}
			}
			//resetting n after each line
			n = 0;
			//incrementing i movies
			i++;
		}
		int structCount = i;
		
		//Sort the movies array using the mergesort function
		mergesort(columnToSort, movies, 0, structCount - 1);

		//Output data by looping through all structs
		//Use new counter to increment through structs
		int k = 0;
		while(k < i )
		{
			fprintf(CSVSorted, "%s,", movies[k].color);
			fprintf(CSVSorted, "%s,", movies[k].director_name);
			fprintf(CSVSorted, "%s,", movies[k].num_critic_for_reviews);
			fprintf(CSVSorted, "%s,", movies[k].duration);
			fprintf(CSVSorted, "%s,", movies[k].director_facebook_likes);
			fprintf(CSVSorted, "%s,", movies[k].actor_3_facebook_likes);
			fprintf(CSVSorted, "%s,", movies[k].actor_2_name);
			fprintf(CSVSorted, "%s,", movies[k].actor_1_facebook_likes);
			fprintf(CSVSorted, "%s,", movies[k].gross);
			fprintf(CSVSorted, "%s,", movies[k].genres);
			fprintf(CSVSorted, "%s,", movies[k].actor_1_name);
			fprintf(CSVSorted, "\"%s\",", movies[k].movie_title);
			fprintf(CSVSorted, "%s,", movies[k].num_voted_users);
			fprintf(CSVSorted, "%s,", movies[k].cast_total_facebook_likes);
			fprintf(CSVSorted, "%s,", movies[k].actor_3_name);
			fprintf(CSVSorted, "%s,", movies[k].facenumber_in_poster);
			fprintf(CSVSorted, "%s,", movies[k].plot_keywords);
			fprintf(CSVSorted, "%s,", movies[k].movie_imdb_link);
			fprintf(CSVSorted, "%s,", movies[k].num_user_for_reviews);
			fprintf(CSVSorted, "%s,", movies[k].language);
			fprintf(CSVSorted, "%s,", movies[k].country);
			fprintf(CSVSorted, "%s,", movies[k].content_rating);
			fprintf(CSVSorted, "%s,", movies[k].budget);
			fprintf(CSVSorted, "%s,", movies[k].title_year);
			fprintf(CSVSorted, "%s,", movies[k].actor_2_facebook_likes);
			fprintf(CSVSorted, "%s,", movies[k].imdb_score);
			fprintf(CSVSorted, "%s,", movies[k].aspect_ratio);
			fprintf(CSVSorted, "%s,", movies[k].movie_facebook_likes);
			fprintf(CSVSorted, "\n");
			k++;
		}

		//freeing mallocs	
		free(CSV);
		free(trimBuffer);
		free(movies);

		//returns status 1 so this 1 process can be added to process count in parent
		exit(1);

	} else {
		// parent
		// add finishing touches to program and print the process count
		if(initialpid == getpid())
		{
			waitpid(-1, &status, 0);
			processcount += WEXITSTATUS(status);
			printf("\nTotal number of processes: %d\n", processcount);
		}
	}
	
	return 0;	

}
//function for trimming whitespace, accepts and returns char*
char* trim(char *str) 
{
    char* end = str + strlen(str) - 1;
    while(*str && isspace(*str)) str++;
    while(end > str && isspace(*end)) *end-- = '\0';
    return str;
}
