require 'rubygems'
require 'json/pure'
require 'time'
require 'pp'
require 'tweetstream'
require 'socket'


# a very simple classified for a twitter search

class Collector
  attr_accessor :counts, :mentions, :hashtag, :countries, :socket, :stop_words
  attr_reader :dump_data_for_vista

  def initialize

# requires something to consume the data - 
# if it doesn't find a tcp socket will just print the data

    hostname = "localhost"
    port = 8082

    begin
       @socket = TCPSocket.new(hostname, port)
    rescue SystemCallError => e
       STDERR.puts(e)
       puts "no socket: will just print out data"
       @socket = nil
    end

    self.countries = load_countries("countries.txt")
    self.stop_words = load("stopList.txt")

    self.counts = {}
    self.mentions = {}

    self.hashtag = "#eurovision"

    self.countries.each do |k,v|
        puts k
        self.counts[k]=0
        self.mentions[k]=0
    end

    start_twitter_listen
  end


# resets all counters

  def clear_counts()

    self.countries.each do |k,v|
        self.counts[k]=0
        self.mentions[k]=0
    end

  end


# Listens to twitter stream via the search for the hastag
# Classifies countries according to countries.txt, a tab-separated list 
#  of countries, artists, songs etc
# Dumps out the data after each classification either to a socket or to std out

  def start_twitter_listen

      # add your app's tokens in here

      TweetStream.configure do |config|
        config.consumer_key       = ''
        config.consumer_secret    = ''
        config.oauth_token        = ''
        config.oauth_token_secret = ''
        config.auth_method        = :oauth
        config.parser   = :json_pure
      end

      total = 0

      TweetStream::Client.new.track(self.hashtag,"##{self.hashtag}",
        :limit => Proc.new{ |status_id, user_id| 
           puts "limit!"
           sleep 5 
           },
        :error => Proc.new{ |status_id, user_id| 
           puts "error!" 
           sleep 5
           },
        ) do |status|
        begin
          clear_counts()
          total = 0
          str = status.text
          res_c = country_classify(str, stop_words, countries)
          if(res_c && res_c.length>0)
            puts "**classified country #{res_c}"

            res_c.each do |b|
                  c = self.mentions[b] 
                  puts "got a count for #{b} which is #{c}"
                  d = c+1
                  self.mentions[b]=d
            end

          else
            # classification failed for the tweet, but it did match the hashtag 
            # so we keep the total
            total = total+1
            puts "**no country found but total is #{total}"
          end

        rescue JSON::ParserError
          puts "parser error - don't worry"
          
        rescue Exception => e
          puts "error: #{e.class.name}\n #{e}"
          puts e.backtrace
          puts "waiting for a bit then reconnecting"
          sleep 370
        end

        dump_data_for_vista(total)

      end

  end


# loads a file

  def load(filen)
        arr = []
        file = File.new(filen, "r")
        while (line = file.gets)
           arr << line.strip!
        end
        return arr

  end

# loads countries file

  def load_countries(filen)
        hash = {}
        file = File.new(filen, "r")
        while (line = file.gets)
           line.strip!
           line.downcase!
           arr = line.split("\t")
           k = arr[0]
           v = arr.length
           hash[k]=arr
        end
        return hash
  end


# attempts to classify a piece of text with respect to a country list

  def country_classify(str, stop_words, countries)
   str = str.downcase
   arr = str.split(" ")
   to_classify = arr - stop_words
   countries_found = []
   countries.each do |k,v|
     c_len = v.length
     c = v - to_classify
     #puts "countries found are: #{c}"
     if(c && c.length<c_len)
        countries_found.push(k)
     end
   end
   if(countries_found.length>0)
     return countries_found
   else
     return nil
   end
  end


# dumps the data out in a suitable format, to a socket or to std out

  def dump_data_for_vista(total)

    config = {}
    config["tvByPlatform"]={}

    config2 = {}
    config2["simulcast"]={}

    t = Time.now.utc
    tstr = t.strftime("%Y-%m-%dT%TZ")

    puts "timestamp #{tstr}"

    config["tvByPlatform"]["timestamp"] = tstr
    config2["simulcast"]["timestamp"] = tstr

    config["tvByPlatform"]["services"] = {}
    config2["simulcast"]["services"] = []

    self.mentions.each do |k,v|
      total = total+v
      config["tvByPlatform"]["services"][k] = {"twitter"=>v}
      config2["simulcast"]["services"].push({"id"=>k,"count"=>v})
    end

    config["tvByPlatform"]["streams"] = total
    config2["simulcast"]["streams"] = total

    config["tvByPlatform"]["services"]["total"] = {"twitter" => total}
    config2["simulcast"]["services"].push({"id"=>"total","count"=> total})

    str =  JSON.generate(config)
    str2 =  JSON.generate(config2)

    if(@socket)
      @socket.puts(str)
      @socket.puts(str2)
    else
      puts str
      puts str2
      puts ""
    end
  end


end



begin

  c = Collector.new
  trap("INT") { 
    if(c.socket)
      c.socket.close
    end
    exit
  }
  
end
