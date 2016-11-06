package com.github.xsocket.study.kafka;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.client.fluent.Request;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * 猫眼票房数据消费者
 * Created by MWQ on 16/11/5.
 */
public class KafkaConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);

  static {
    try {
      Class.forName("com.mysql.jdbc.Driver") ;
    } catch (ClassNotFoundException e) {
      LOGGER.error("Can not initialize Mysql Driver.", e);
    }
  }

  public static void main(String[] arg) throws Exception {
    Consumer<Long, String> consumer = KafkaUtils.newConsumer();
    consumer.subscribe(Arrays.asList(KafkaUtils.TOPIC));
    try {
      while(true) {
        ConsumerRecords<Long, String> records = consumer.poll(100);
        LOGGER.info("counts = {}", records.count());
        for (ConsumerRecord<Long, String> record : records) {
          JSONObject json = JSONObject.parseObject(record.value());
          List<MovieBox> list = parseBox(json, new Date(record.key()));
          saveToMysql(list);
        }
        Thread.sleep(60 * 1000L);
      }
    } finally {
      consumer.close();
    }
  }

  private static List<MovieBox> parseBox(JSONObject maoyan, Date date) {
    List<MovieBox> list = new ArrayList<MovieBox>(16);
    final float unit = 10000.0F;

    // 抓取数据
    MovieBox nation = new MovieBox(0, "全国大盘", date);
    nation.setBoxOffice(new Float(maoyan.getFloat("totalBox") * unit).intValue());
    nation.setBoxOfficeTotal(nation.getBoxOffice());
    nation.setShowTimes(0);
    nation.setTicket(0);
    list.add(nation);

    // 处理猫眼各影片票房
    JSONArray movieArray = maoyan.getJSONArray("data");
    for(int i = 0; i < movieArray.size(); i++) {
      JSONObject my = movieArray.getJSONObject(i);
      MovieBox movie = new MovieBox(my.getInteger("movieId"), my.getString("movieName"), date);
      movie.setBoxOffice(new Float(my.getFloat("dailyBoxOffice") * unit).intValue());
      movie.setBoxOfficeTotal(new Float(my.getFloat("sumBoxOffice") * unit).intValue());
      movie.setShowTimes(my.getInteger("totalShow"));
      movie.setTicket(new Float(my.getFloat("totalViewer") * unit).intValue());
      list.add(movie);
    }

    return list;
  }

  private static void saveToMysql(List<MovieBox> list) throws SQLException {
    Connection conn = null;
    PreparedStatement stat = null;
    try {
      conn = DriverManager.getConnection("jdbc:mysql://vgeek.aliyun:3306/maoyan_v1", "maoyan", "maoyan");

      // prepare SQL
      int count = list.size();
      StringBuilder sql = new StringBuilder(1024);
      sql.append("insert into movie_box_daily (show_date, show_time, movie_id, movie_name, box_office, box_office_total, show_times, ticket, ticks) values ( ?, ?, ?, ?, ?, ?, ?, ?, ? )");
      if(count > 1) {
        for(int i = 1; i < count; i++) {
          sql.append(", ( ?, ?, ?, ?, ?, ?, ?, ?, ? )");
        }
      }

      // prepare STATEMENT
      final int PARA_COUNT = 9;
      stat = conn.prepareStatement(sql.toString());
      for(int i = 0; i < count; i++) {
        MovieBox box = list.get(i);
        stat.setDate(i * PARA_COUNT + 1, new java.sql.Date(box.getTicks()));
        stat.setString(i * PARA_COUNT + 2, box.getShowTime());
        stat.setInt(i * PARA_COUNT + 3, box.getMovieId());
        stat.setString(i * PARA_COUNT + 4, box.getMovieName());
        stat.setInt(i * PARA_COUNT + 5, box.getBoxOffice());
        stat.setInt(i * PARA_COUNT + 6, box.getBoxOfficeTotal());
        stat.setInt(i * PARA_COUNT + 7, box.getShowTimes());
        stat.setInt(i * PARA_COUNT + 8, box.getTicket());
        stat.setLong(i * PARA_COUNT + 9, box.getTicks());
      }

      // EXECUTE
      int update = stat.executeUpdate();
      LOGGER.info("Save {} box data.", update);
    } catch(SQLException e){
      LOGGER.error("Fail to connect mysql.", e);
    } finally {
      if(stat != null) {
        stat.close();
      }
      if(conn != null) {
        conn.close();
      }
    }
  }

  private static class MovieBox {

    private static final DateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ss");

    private Long id;
    private Date showDate;
    private String showTime;
    private Integer movieId;
    private String movieName;
    private Integer boxOffice;
    private Integer boxOfficeTotal;
    private Integer showTimes;
    private Integer ticket;
    private Long ticks;

    public MovieBox(Integer movieId, String movieName, Date datetime) {
      this.movieId = movieId;
      this.movieName = movieName;
      this.showDate = datetime;
      this.showTime = TIME_FORMAT.format(datetime);
      this.ticks = datetime.getTime();
    }

    public Long getId() {
      return id;
    }

    public void setId(Long id) {
      this.id = id;
    }

    public Date getShowDate() {
      return showDate;
    }

    public void setShowDate(Date showDate) {
      this.showDate = showDate;
    }

    public String getShowTime() {
      return showTime;
    }

    public void setShowTime(String showTime) {
      this.showTime = showTime;
    }

    public Integer getMovieId() {
      return movieId;
    }

    public void setMovieId(Integer movieId) {
      this.movieId = movieId;
    }

    public String getMovieName() {
      return movieName;
    }

    public void setMovieName(String movieName) {
      this.movieName = movieName;
    }

    public Integer getBoxOffice() {
      return boxOffice;
    }

    public void setBoxOffice(Integer boxOffice) {
      this.boxOffice = boxOffice;
    }

    public Integer getBoxOfficeTotal() {
      return boxOfficeTotal;
    }

    public void setBoxOfficeTotal(Integer boxOfficeTotal) {
      this.boxOfficeTotal = boxOfficeTotal;
    }

    public Integer getShowTimes() {
      return showTimes;
    }

    public void setShowTimes(Integer showTimes) {
      this.showTimes = showTimes;
    }

    public Integer getTicket() {
      return ticket;
    }

    public void setTicket(Integer ticket) {
      this.ticket = ticket;
    }

    public Long getTicks() {
      return ticks;
    }

    public void setTicks(Long ticks) {
      this.ticks = ticks;
    }
  }
}
