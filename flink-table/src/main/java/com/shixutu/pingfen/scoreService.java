package com.shixutu.pingfen;

/**
 * @Author: daxian
 * @Date: 2021/10/31 5:08 下午
 */
public class scoreService {
    private approveService approveService;
    private mysql mysql;
    public  void  create(){
        publish();
        approveService.agree();
        up();
        mysql.save();
    }

    public  void  publish(){}
    public  void  delete(){}
    public  void  update(){}
    public  void  up(){}
    public  void  down(){}

}
