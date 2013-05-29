twitter-eurovision-stream
=========================

A very simple classifier for a twitter search for a hashtag, originally used for 
Eurovision 2013. You'll need a twitter app's credentials to make it work.

It:

* listens to twitter stream via the search for the hastag using Tweetstream

* classifies countries according to countries.txt, a tab-separated list of 
countries, artists, songs etc

* dumps out the data after each classification either to a socket or to std out.

