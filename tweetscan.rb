require 'rubygems'
require 'bundler'
require 'mysql2'
require 'json'

Bundler.require

require 'twitter/json_stream'
require './secret' if File.exist?("./secret.rb")

# TwitterのAPIキー情報を環境変数から取得
TWITTER_CONSUMER_KEY        ||= ENV['TWITTER_CONSUMER_KEY']
TWITTER_CONSUMER_SECRET     ||= ENV['TWITTER_CONSUMER_SECRET']
TWITTER_OAUTH_TOKEN         ||= ENV['TWITTER_OAUTH_TOKEN']
TWITTER_OAUTH_TOKEN_SECRET  ||= ENV['TWITTER_OAUTH_TOKEN_SECRET']
FOLLOWS                     ||= ENV['FOLLOWS']

# DBへの接続情報を環境変数から取得
DB_HOSTNAME   ||= ENV['DB_HOSTNAME']
DB_USER_NAME  ||= ENV['DB_USER_NAME']
DB_PASSWORD   ||= ENV['DB_PASSWORD']
DB_NAME       ||= ENV['DB_NAME']

EventMachine::run {
  stream = Twitter::JSONStream.connect(
    :path    => "/1.1/statuses/filter.json?follow=#{FOLLOWS}",
    :oauth => {
      :consumer_key    => TWITTER_CONSUMER_KEY,
      :consumer_secret => TWITTER_CONSUMER_SECRET,
      :access_key      => TWITTER_OAUTH_TOKEN,
      :access_secret   => TWITTER_OAUTH_TOKEN_SECRET
    },
    :ssl => true
  )

  stream.each_item do |item|
    $stdout.print "item: #{item}\n"
    $stdout.flush

    # MySQLへ接続(Postgresなどを使う場合は適宜変更)
    client = Mysql2::Client.new(:host => DB_HOSTNAME, :username => DB_USER_NAME, :password => DB_PASSWORD || '', :database => DB_NAME)

    # Tweetのjsonをパース
    tw_json = JSON.parse(item)

    # DBに格納するためにエンコーディング
    user_id                         = client.escape(tw_json['user']['id_str'])
    user_name                       = client.escape(tw_json['user']['name'])
    user_screen_name                = client.escape(tw_json['user']['screen_name'])
    user_image                      = client.escape(tw_json['user']['profile_image_url'])
    user_description                = client.escape(tw_json['user']['description']) rescue nil
    text                            = client.escape(tw_json['text'])
    post_media_url                  = client.escape(tw_json['entities']['media'].first['media_url']) rescue nil
    twitter_status_id               = client.escape(tw_json['id_str'])
    twitter_reply_status_id         = client.escape(tw_json['in_reply_to_status_id_str']) rescue nil
    twitter_reply_user_id           = client.escape(tw_json['in_reply_to_user_id_str'])   rescue nil
    twitter_reply_user_screen_name  = client.escape(tw_json['in_reply_to_screen_name'])   rescue nil

    # tweetsテーブルに書き込み
    client.query("INSERT INTO tweets (user_id, user_name, user_screen_name, text, post_media_url, user_image, user_description, twitter_status_id, twitter_reply_status_id, twitter_reply_user_id, twitter_reply_user_screen_name, updated_at, created_at) VALUES ('#{user_id}', '#{user_name}', '#{user_screen_name}', '#{text}', '#{post_media_url}', '#{user_image}', '#{user_description}', '#{twitter_status_id}', '#{twitter_reply_status_id}', '#{twitter_reply_user_id}', '#{twitter_reply_user_screen_name}', '#{Time.now}', '#{Time.now}')")

    # MySQLとの接続を解除
    client.close
  end

  stream.on_error do |message|
    $stdout.print "error: #{message}\n"
    $stdout.flush
  end

  # 再接続は書いていないです。書いて教えてくださいw
  stream.on_reconnect do |timeout, retries|
    $stdout.print "reconnecting in: #{timeout} seconds\n"
    $stdout.flush
  end
  
  stream.on_max_reconnects do |timeout, retries|
    $stdout.print "Failed after #{retries} failed reconnects\n"
    $stdout.flush
  end
}

