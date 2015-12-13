rm TopMoviesInGenres.txt

echo "-----------------------------" >> TopMoviesInGenres.txt
echo "" >> MovieLensDataMetrics.txt
hadoop fs -cat TopMoviesInGenres/part-00000 | head -n 10 >> TopMoviesInGenres.txt
echo "" >> MovieLensDataMetrics.txt
hadoop fs -cat TopMoviesInGenres/part-00001 | head -n 10 >> TopMoviesInGenres.txt
echo "" >> MovieLensDataMetrics.txt
hadoop fs -cat TopMoviesInGenres/part-00002 | head -n 10 >> TopMoviesInGenres.txt
echo "" >> MovieLensDataMetrics.txt
hadoop fs -cat TopMoviesInGenres/part-00003 | head -n 10 >> TopMoviesInGenres.txt 
echo "" >> MovieLensDataMetrics.txt
hadoop fs -cat TopMoviesInGenres/part-00004 | head -n 10 >> TopMoviesInGenres.txt 
echo "" >> MovieLensDataMetrics.txt
hadoop fs -cat TopMoviesInGenres/part-00005 | head -n 10 >> TopMoviesInGenres.txt
echo "" >> MovieLensDataMetrics.txt
hadoop fs -cat TopMoviesInGenres/part-00006 | head -n 10 >> TopMoviesInGenres.txt
echo "" >> MovieLensDataMetrics.txt
hadoop fs -cat TopMoviesInGenres/part-00007 | head -n 10 >> TopMoviesInGenres.txt
echo "" >> MovieLensDataMetrics.txt
hadoop fs -cat TopMoviesInGenres/part-00008 | head -n 10 >> TopMoviesInGenres.txt
echo "" >> MovieLensDataMetrics.txt
hadoop fs -cat TopMoviesInGenres/part-00009 | head -n 10 >> TopMoviesInGenres.txt
echo "" >> MovieLensDataMetrics.txt
hadoop fs -cat TopMoviesInGenres/part-00010 | head -n 10 >> TopMoviesInGenres.txt
echo "" >> MovieLensDataMetrics.txt
hadoop fs -cat TopMoviesInGenres/part-00011 | head -n 10 >> TopMoviesInGenres.txt
echo "" >> MovieLensDataMetrics.txt
hadoop fs -cat TopMoviesInGenres/part-00012 | head -n 10 >> TopMoviesInGenres.txt
echo "" >> MovieLensDataMetrics.txt
hadoop fs -cat TopMoviesInGenres/part-00013 | head -n 10 >> TopMoviesInGenres.txt 
echo "" >> MovieLensDataMetrics.txt
hadoop fs -cat TopMoviesInGenres/part-00014 | head -n 10 >> TopMoviesInGenres.txt
echo "" >> MovieLensDataMetrics.txt
hadoop fs -cat TopMoviesInGenres/part-00015 | head -n 10 >> TopMoviesInGenres.txt 
echo "" >> MovieLensDataMetrics.txt
hadoop fs -cat TopMoviesInGenres/part-00016 | head -n 10 >> TopMoviesInGenres.txt
echo "" >> MovieLensDataMetrics.txt
hadoop fs -cat TopMoviesInGenres/part-00017 | head -n 10 >> TopMoviesInGenres.txt
echo "" >> MovieLensDataMetrics.txt
hadoop fs -cat TopMoviesInGenres/part-00018 | head -n 10 >> TopMoviesInGenres.txt
echo "" >> MovieLensDataMetrics.txt
hadoop fs -cat TopMoviesInGenres/part-00019 | head -n 10 >> TopMoviesInGenres.txt             
