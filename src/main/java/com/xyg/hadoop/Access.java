package com.xyg.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 自定义复杂数据类型
 *  1. 按照Hadoop的规范，实现writable接口
 *  2. 实现write和readFileds两个方法: 注意read的顺序是按照写的顺序进行的。
 *  3. 定义一个默认的构造方法
 * @author Truman
 * @version [版本号, 2019年11月23日]
 */
public class Access implements Writable
{
    private String phone;
    private int downStream;
    private int upStream;
    private int total;
    
    public String getPhone() {
        return phone;
    }
    public void setPhone(String phone) {
        this.phone = phone;
    }
    public int getDownStream() {
        return downStream;
    }
    public void setDownStream(int downStream) {
        this.downStream = downStream;
    }
    public int getUpStream() {
        return upStream;
    }
    public void setUpStream(int upStream) {
        this.upStream = upStream;
    }
    public int getTotal() {
        return total;
    }
    public void setTotal(int total) {
        this.total = total;
    }
    
    public Access() {

    }
    
    public Access(String phone, int downStream, int upStream) {
        this.phone = phone;
        this.downStream = downStream;
        this.upStream = upStream;
        this.total = downStream + upStream;
    }
    
    
    @Override
    public String toString() {
        return "Access [phone=" + phone + ", downStream=" + downStream + ", upStream=" + upStream + ", total=" + total
                + "]";
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeInt(downStream);
        out.writeInt(upStream);
        out.writeInt(total);
    }
   
    @Override
    public void readFields(DataInput in) throws IOException {
        this.phone = in.readUTF();
        this.downStream = in.readInt();
        this.upStream = in.readInt();
        this.total = in.readInt();
    }
}
