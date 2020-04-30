package org.data.movie.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * 存储每行数据的
 *
 * @author Movie 类要作为map端的value传输 实现序列化 Writable 序列化的接口
 */
public class MovierReviewBean implements Writable {
    private String director;
    private String img;
    private String hot;
    private String play_time;
    private String description;
    private String stars;
    private String score;
    private String alias;
    private String tags;
    private String short_desc;
    private String name;
    private String play_url;

    public void set(String director, String img, String hot, String play_time, String description,
                    String stars, String score, String alias, String tags, String short_desc, String name, String play_url) {
        this.director = director;
        this.img = img;
        this.hot = hot;
        this.play_time = play_time;
        this.description = description;
        this.stars = stars;
        this.score = score;
        this.alias = alias;
        this.tags = tags;
        this.short_desc = short_desc;
        this.name = name;
        this.play_url = play_url;
    }

    @Override
    public String toString() {
        return "MovierReviewBean[" +
                "director='" + director + '\'' +
                ", img='" + img + '\'' +
                ", hot='" + hot + '\'' +
                ", play_time='" + play_time + '\'' +
                ", description='" + description + '\'' +
                ", stars='" + stars + '\'' +
                ", score='" + score + '\'' +
                ", alias='" + alias + '\'' +
                ", tags='" + tags + '\'' +
                ", short_desc='" + short_desc + '\'' +
                ", name='" + name + '\'' +
                ", play_url='" + play_url + '\'' +
                ']';
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // 写出去的类型和顺序
        out.writeUTF(director);
        out.writeUTF(img);
        out.writeUTF(hot);
        out.writeUTF(play_time);
        out.writeUTF(description);
        out.writeUTF(stars);
        out.writeUTF(score);
        out.writeUTF(alias);
        out.writeUTF(tags);
        out.writeUTF(short_desc);
        out.writeUTF(name);
        out.writeUTF(play_url);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.director = in.readUTF();
        this.img = in.readUTF();
        this.hot = in.readUTF();
        this.play_time = in.readUTF();
        this.description = in.readUTF();
        this.stars = in.readUTF();
        this.score = in.readUTF();
        this.alias = in.readUTF();
        this.tags = in.readUTF();
        this.short_desc = in.readUTF();
        this.name = in.readUTF();
        this.play_url = in.readUTF();
    }

    public String getDirector() {
        return director;
    }

    public void setDirector(String director) {
        this.director = director;
    }

    public String getImg() {
        return img;
    }

    public void setImg(String img) {
        this.img = img;
    }

    public String getHot() {
        return hot;
    }

    public void setHot(String hot) {
        this.hot = hot;
    }

    public String getPlay_time() {
        return play_time;
    }

    public void setPlay_time(String play_time) {
        this.play_time = play_time;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getStars() {
        return stars;
    }

    public void setStars(String stars) {
        this.stars = stars;
    }

    public String getScore() {
        return score;
    }

    public void setScore(String score) {
        this.score = score;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public String getShort_desc() {
        return short_desc;
    }

    public void setShort_desc(String short_desc) {
        this.short_desc = short_desc;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPlay_url() {
        return play_url;
    }

    public void setPlay_url(String play_url) {
        this.play_url = play_url;
    }
}
