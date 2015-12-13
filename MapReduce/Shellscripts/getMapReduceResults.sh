#!/bin/bash

# Top 100 Movies
rm MovieLensResults.txt
echo "-----------------------------" >> MovieLensResults.txt
echo "" >> MovieLensResults.txt
echo "Top 100 Movies" >> MovieLensResults.txt
hadoop fs -cat sortedmovietitlesbyrating/* | head -n 100 >> MovieLensResults.txt
echo "" >> MovieLensResults.txt
echo "-----------------------------" >> MovieLensResults.txt
echo "" >> MovieLensResults.txt
echo "Genres sorted by average rating" >> MovieLensResults.txt
hadoop fs -cat sortedmoviegenresbyrating/* >> MovieLensResults.txt
echo "" >> MovieLensResults.txt
echo "-----------------------------" >> MovieLensResults.txt
echo "" >> MovieLensResults.txt
echo "Movie rating frequency" >> MovieLensResults.txt
hadoop fs -cat sortedmovieratingfrequency/* >> MovieLensResults.txt
echo "" >> MovieLensResults.txt
echo "-----------------------------" >> MovieLensResults.txt
echo "" >> MovieLensResults.txt
echo "Top 10 Movies within each genre" >> MovieLensResults.txt

# Action
echo "" >> MovieLensResults.txt
echo "Action" >> MovieLensResults.txt
hadoop fs -cat movieratingstitlesbygenreout/Action/sorted/* | head -n 10 >> MovieLensResults.txt
echo "-----------------------------" >> MovieLensResults.txt
echo "" >> MovieLensResults.txt

# Adventure
echo "" >> MovieLensResults.txt
echo "Adventure" >> MovieLensResults.txt
hadoop fs -cat movieratingstitlesbygenreout/Adventure/sorted/* | head -n 10 >> MovieLensResults.txt
echo "-----------------------------" >> MovieLensResults.txt
echo "" >> MovieLensResults.txt

# Animation
echo "" >> MovieLensResults.txt
echo "Animation" >> MovieLensResults.txt
hadoop fs -cat movieratingstitlesbygenreout/Animation/sorted/* | head -n 10 >> MovieLensResults.txt
echo "-----------------------------" >> MovieLensResults.txt
echo "" >> MovieLensResults.txt

# Children
echo "" >> MovieLensResults.txt
echo "Children" >> MovieLensResults.txt
hadoop fs -cat movieratingstitlesbygenreout/Children/sorted/* | head -n 10 >> MovieLensResults.txt
echo "-----------------------------" >> MovieLensResults.txt
echo "" >> MovieLensResults.txt

# Comedy
echo "" >> MovieLensResults.txt
echo "Comedy" >> MovieLensResults.txt
hadoop fs -cat movieratingstitlesbygenreout/Comedy/sorted/* | head -n 10 >> MovieLensResults.txt
echo "-----------------------------" >> MovieLensResults.txt
echo "" >> MovieLensResults.txt

# Crime
echo "" >> MovieLensResults.txt
echo "Crime" >> MovieLensResults.txt
hadoop fs -cat movieratingstitlesbygenreout/Crime/sorted/* | head -n 10 >> MovieLensResults.txt
echo "-----------------------------" >> MovieLensResults.txt
echo "" >> MovieLensResults.txt

# Documentary
echo "" >> MovieLensResults.txt
echo "Documentary" >> MovieLensResults.txt
hadoop fs -cat movieratingstitlesbygenreout/Documentary/sorted/* | head -n 10 >> MovieLensResults.txt
echo "-----------------------------" >> MovieLensResults.txt
echo "" >> MovieLensResults.txt

# Drama
echo "" >> MovieLensResults.txt
echo "Drama" >> MovieLensResults.txt
hadoop fs -cat movieratingstitlesbygenreout/Drama/sorted/* | head -n 10 >> MovieLensResults.txt
echo "-----------------------------" >> MovieLensResults.txt
echo "" >> MovieLensResults.txt

# Fantasy
echo "" >> MovieLensResults.txt
echo "Fantasy" >> MovieLensResults.txt
hadoop fs -cat movieratingstitlesbygenreout/Fantasy/sorted/* | head -n 10 >> MovieLensResults.txt
echo "-----------------------------" >> MovieLensResults.txt
echo "" >> MovieLensResults.txt

# Film-Noir
echo "" >> MovieLensResults.txt
echo "Film-Noir" >> MovieLensResults.txt
hadoop fs -cat movieratingstitlesbygenreout/Film-Noir/sorted/* | head -n 10 >> MovieLensResults.txt
echo "-----------------------------" >> MovieLensResults.txt
echo "" >> MovieLensResults.txt

# Horror
echo "" >> MovieLensResults.txt
echo "Horror" >> MovieLensResults.txt
hadoop fs -cat movieratingstitlesbygenreout/Horror/sorted/* | head -n 10 >> MovieLensResults.txt
echo "-----------------------------" >> MovieLensResults.txt
echo "" >> MovieLensResults.txt

# IMAX
echo "" >> MovieLensResults.txt
echo "IMAX" >> MovieLensResults.txt
hadoop fs -cat movieratingstitlesbygenreout/IMAX/sorted/* | head -n 10 >> MovieLensResults.txt
echo "-----------------------------" >> MovieLensResults.txt
echo "" >> MovieLensResults.txt

# Musical
echo "" >> MovieLensResults.txt
echo "Musical" >> MovieLensResults.txt
hadoop fs -cat movieratingstitlesbygenreout/Musical/sorted/* | head -n 10 >> MovieLensResults.txt
echo "-----------------------------" >> MovieLensResults.txt
echo "" >> MovieLensResults.txt

# Mystery
echo "" >> MovieLensResults.txt
echo "Mystery" >> MovieLensResults.txt
hadoop fs -cat movieratingstitlesbygenreout/Mystery/sorted/* | head -n 10 >> MovieLensResults.txt
echo "-----------------------------" >> MovieLensResults.txt
echo "" >> MovieLensResults.txt

# None
echo "" >> MovieLensResults.txt
echo "None" >> MovieLensResults.txt
hadoop fs -cat movieratingstitlesbygenreout/None/sorted/* | head -n 10 >> MovieLensResults.txt
echo "-----------------------------" >> MovieLensResults.txt
echo "" >> MovieLensResults.txt

# Romance
echo "" >> MovieLensResults.txt
echo "Romance" >> MovieLensResults.txt
hadoop fs -cat movieratingstitlesbygenreout/Romance/sorted/* | head -n 10 >> MovieLensResults.txt
echo "-----------------------------" >> MovieLensResults.txt
echo "" >> MovieLensResults.txt

# Sci-Fi
echo "" >> MovieLensResults.txt
echo "Sci-Fi" >> MovieLensResults.txt
hadoop fs -cat movieratingstitlesbygenreout/Sci-Fi/sorted/* | head -n 10 >> MovieLensResults.txt
echo "-----------------------------" >> MovieLensResults.txt
echo "" >> MovieLensResults.txt

# Thriller
echo "" >> MovieLensResults.txt
echo "Thriller" >> MovieLensResults.txt
hadoop fs -cat movieratingstitlesbygenreout/Thriller/sorted/* | head -n 10 >> MovieLensResults.txt
echo "-----------------------------" >> MovieLensResults.txt
echo "" >> MovieLensResults.txt

# War
echo "" >> MovieLensResults.txt
echo "War" >> MovieLensResults.txt
hadoop fs -cat movieratingstitlesbygenreout/War/sorted/* | head -n 10 >> MovieLensResults.txt
echo "-----------------------------" >> MovieLensResults.txt
echo "" >> MovieLensResults.txt

# Western
echo "" >> MovieLensResults.txt
echo "Western" >> MovieLensResults.txt
hadoop fs -cat movieratingstitlesbygenreout/Western/sorted/* | head -n 10 >> MovieLensResults.txt
echo "-----------------------------" >> MovieLensResults.txt
echo "" >> MovieLensResults.txt

 
