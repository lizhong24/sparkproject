package com.wolf.sparkproject.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.wolf.sparkproject.conf.ConfigurationManager;
import com.wolf.sparkproject.constant.Constants;
import com.wolf.sparkproject.dao.*;
import com.wolf.sparkproject.dao.factory.DAOFactory;
import com.wolf.sparkproject.domain.*;
import com.wolf.sparkproject.test.MockData;
import com.wolf.sparkproject.util.*;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import com.google.common.base.Optional;

import java.util.*;

/**
 * 用户访问session分析spark作业
 *
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 * 1.时间范围：起始日期-结束日期
 * 2.性别：男或女
 * 3.年龄范围
 * 4.职业：多选
 * 5.城市：多选
 * 6.搜索词：多个搜索词，只要某个session中的任何一个
 *   action搜索过指定的关键词，那么session就符合条件
 * 7.点击品类：多个品类，只要某个session中的任何一个
 *   action点击过某个品类，那么session就符合条件
 *
 * 我们的Spark作业如何接受用户创建的任务呢？
 * J2EE平台在接收用户创建任务的请求之后，会将任务信息插入MySQL的task表中，
 * 任务参数以JSON格式封装在task_param字段中
 * 接着J2EE平台执行我们的spark-submit shell脚本，并将taskId作为参数传递给spark-submit shell脚本
 * spark-submit shell脚本，在执行时，是可以接收参数的，并且会将接收的参数传递给spark作业的main函数
 * 参数就封装在main函数得到args数组中
 *
 * 这是spark本身提供的特性
 */
public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) {
        args = new String[]{"2"};
        //构建spark上下文

        //首先在Constants.java中设置spark作业相关的常量
        //String SPARK_APP_NAME = "UserVisitSessionAnalyzeSpark";
        //保存Constants.java配置

        /**
         * Kryo 序列化原因
         在广播大变量进行优化后，还可以进一步优化，即优化这个序列化格式。
         默认情况下，Spark内部是使用Java的序列化机制ObjectOutputStream / ObjectInputStream这种对象输入输出流机制来进行序列化。
         这种默认序列化机制的好处在于：处理起来比较方便，也不需要我们手动去做什么事情，只是在算子里面使用的变量必须是实现Serializable接口的，可序列化即可。
         但是缺点在于：默认的序列化机制的效率不高，序列化的速度比较慢；序列化以后的数据，占用的内存空间相对还是比较大。

         可以手动进行序列化格式的优化，Spark支持使用Kryo序列化机制。Kryo序列化机制比默认的Java序列化机制速度要快，序列化后的数据要更小，大概是Java序列化机制的1/10。所以Kryo序列化优化以后，可以让网络传输的数据变少，在集群中耗费的内存资源大大减少。

         Kryo序列化生效位置
         Kryo序列化机制一旦启用以后，会在以下几个地方生效：
         1. 算子函数中使用到的外部变量，会序列化，这时可以是用Kryo序列化机制；
         2. 持久化 RDD 时进行序列化，比如StorageLevel.MEMORY_ONLY_SER，可以使用 Kryo 进一步优化序列化的效率和性能；
         3、进行shuffle时，比如在进行stage间的task的shuffle操作时，节点与节点之间的task会互相大量通过网络拉取和传输文件，此时，这些数据既然通过网络传输，也是可能要序列化的，就会使用Kryo。

         Kryo序列化优点
         1. 算子函数中使用到的外部变量，使用Kryo以后：优化网络传输的性能，可以优化集群中内存的占用和消耗；
         2. 持久化RDD，优化内存的占用和消耗，持久化RDD占用的内存越少，task执行的时候，创建的对象，就不至于频繁的占满内存，频繁发生GC；
         3、shuffle：可以优化网络传输的性能。

         实现Kryo序列化步骤
         第一步，在 SparkConf中设置一个属性spark.serializer，使用org.apache.spark.serializer.KryoSerializer类。
         Kryo 之所以没有被作为默认的序列化类库的原因，主要是因为 Kryo 要求，如果要达到它的最佳性能的话，那么就一定要注册你自定义的类（比如，你的算子函数中使用到了外部自定义类型的对象变量，这时就要求必须注册你的类，否则 Kryo 达不到最佳性能）。
         第二步，注册你使用到的需要通过 Kryo 序列化的一些自定义类。
         */
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME)
                .setMaster("local")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{
                        CategorySortKey.class});

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = getSQLContext(sc.sc());

        //生成模拟测试数据
        mockData(sc, sqlContext);

        //创建需要使用的DAO组件
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();

        //那么就首先得查询出来指定的任务，并获取任务的查询参数
        long taskid = ParamUtils.getTaskIdFromArgs(args);
        Task task = taskDAO.findById(taskid);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        System.out.println(taskid);
        System.out.println(taskParam);

        //如果要进行session粒度的数据聚合，
        //首先要从user_visit_action表中，查询出来指定日期范围内的数据
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);
        JavaPairRDD<String, Row> sessionid2ActionRDD = getSessionid2ActionRDD(actionRDD);

        /**
         * 持久化
         * 持久化 sessionid2ActionRDD
         * 因为 sessionid2ActionRDD 使用了两次，所以要对 sessionid2ActionRDD 进行持久化操作。
         * 持久化只需对RDD调用persist()方法，并传入一个持久化级别即可。
         * persist(StorageLevel.MEMORY_ONLY())，纯内存，无序列化，可以用cache()方法来替代
         * StorageLevel.MEMORY_ONLY_SER()，纯内存，序列化，第二选择
         * StorageLevel.MEMORY_AND_DISK()，内存 + 磁盘，无序列号，第三选择
         * StorageLevel.MEMORY_AND_DISK_SER()，内存 + 磁盘，序列化，第四选择
         * StorageLevel.DISK_ONLY()，纯磁盘，第五选择
         * 如果内存充足，要使用双副本高可靠机制， 选择后缀带_2的策略，比如:StorageLevel.MEMORY_ONLY_2()
         */
        sessionid2ActionRDD = sessionid2ActionRDD.persist(StorageLevel.MEMORY_ONLY());

        //聚合
        //首先，可以将行为数据按照session_id进行groupByKey分组
        //此时的数据粒度就是session粒度了，然后可以将session粒度的数据与用户信息数据进行join
        //然后就可以获取到session粒度的数据，同时数据里面还包含了session对应的user信息
        //到这里为止，获取的数据是<sessionid,(sessionid,searchKeywords,
        //clickCategoryIds,age,professional,city,sex)>
        JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(sqlContext, sessionid2ActionRDD);

        //接着，就要针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
        //相当于我们自己编写的算子，是要访问外面的任务参数对象的
        //匿名内部类（算子函数），访问外部对象，是要给外部对象使用final修饰的

        //重构，同时进行过滤和统计
        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator(
                "", new SesssionAggrStatAccumulator());

        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(
                sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);

        //持久化 filteredSessionid2AggrInfoRDD
        filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());

        //生成公共RDD：通过筛选条件的session的访问明细数据
        JavaPairRDD<String, Row> sessionid2detailRDD = getSessionid2detailRDD(
                filteredSessionid2AggrInfoRDD, sessionid2ActionRDD);

        //持久化 sessionid2detailRDD
        sessionid2detailRDD = sessionid2detailRDD.persist(StorageLevel.MEMORY_ONLY());

        randomExtractSession(sc, task.getTaskid(), filteredSessionid2AggrInfoRDD, sessionid2ActionRDD);

        //计算出各个范围的session占比，并写入MySQL
        //calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskid());

        //获取top10热门品类
        List<Tuple2<CategorySortKey, String>> top10CategoryList =
                getTop10Category(task.getTaskid(), sessionid2detailRDD);

        //获取top10活跃session
        getTop10Session(sc, task.getTaskid(), top10CategoryList, sessionid2detailRDD);

        //关闭spark上下文
        sc.close();
    }

    /**
     * 获取SQLContext
     * 如果在本地测试环境的话，那么就生成SQLContext对象
     * 如果在生产环境运行的话，那么就生成HiveContext对象
     * @param sc SparkContext
     * @return SQLContext
     */
    private static SQLContext getSQLContext(SparkContext sc) {
        //在my.properties中配置
        //spark.local=true（打包之前改为flase）
        //在ConfigurationManager.java中添加
        //public static Boolean getBoolean(String key) {
        //  String value = getProperty(key);
        //  try {
        //      return Boolean.valueOf(value);
        //  } catch (Exception e) {
        //      e.printStackTrace();
        //  }
        //  return false;
        //}
        //在Contants.java中添加
        //String SPARK_LOCAL = "spark.local";

        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local) {
            return new SQLContext(sc);
        }else {
            return new HiveContext(sc);
        }
    }

    /**
     * 生成模拟数据
     * 只有是本地模式，才会生成模拟数据
     * @param sc
     * @param sqlContext
     *
     * 生成数据格式：
     * [2019-02-26,29,09094d20e0854c1a89649c6ee77d12d9,6,2019-02-26 7:39:38,null,null,null,79,60,null,null]
     * [0,user0,name0,57,professional97,city99,female]
     */
    private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local) {
            MockData.mock(sc, sqlContext);
        }
    }

    /**
     * 获取指定日期范围内的用户访问行为数据
     * @param sqlContext SQLContext
     * @param taskParam 任务参数
     * @return 行为数据RDD
     */
    private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam) {
        //先在Constants.java中添加任务相关的常量
        //String PARAM_START_DATE = "startDate";
        //String PARAM_END_DATE = "endDate";
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        String sql = "select * "
                + "from user_visit_action "
                + " where date >= '" + startDate + "'"
                + " and date <= '" + endDate + "'";

        DataFrame actionDF = sqlContext.sql(sql);

        return actionDF.javaRDD();
    }

    /**
     * 获取sessionid2到访问行为数据的映射的RDD
     * @param actionRDD 行为数据RDD
     * @return ssionid2到访问行为数据的映射的RDD
     */
    private static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapToPair(new PairFunction<Row, String, Row>(){

            private static final long serialVersionUID = 1L;

            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(2), row);
            }
        });
    }

    /**
     * 对行为数据按sesssion粒度进行聚合
     * @param sessionid2ActionRDD 行为数据RDD
     * @return session粒度聚合数据
     */
    private static JavaPairRDD<String,String> aggregateBySession(
            SQLContext sqlContext, JavaPairRDD<String, Row> sessionid2ActionRDD) {

        //对行为数据按照session粒度进行分组
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessionid2ActionRDD.groupByKey();

        //对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
        //到此为止，获取的数据格式如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(
            new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {

                private static final long serialVersionUID = 1L;

                @Override
                public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple)
                        throws Exception {
                    String sessionid = tuple._1;
                    Iterator<Row> iterator = tuple._2.iterator();

                    StringBuffer searchKeywordsBuffer = new StringBuffer("");
                    StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

                    Long userid = null;

                    //session的起始和结束时间
                    Date startTime = null;
                    Date endTime = null;
                    //session的访问步长
                    int stepLength = 0;

                    //遍历session所有的访问行为
                    while(iterator.hasNext()) {
                        //提取每个 访问行为的搜索词字段和点击品类字段
                        Row row = iterator.next();
                        if(userid == null) {
                            userid = row.getLong(1);
                        }
                        String searchKeyword = row.getString(5);
                        Long clickCategoryId = row.getLong(6);

                        //实际上这里要对数据说明一下
                        //并不是每一行访问行为都有searchKeyword和clickCategoryId两个字段的
                        //其实，只有搜索行为是有searchKeyword字段的
                        //只有点击品类的行为是有clickCaregoryId字段的
                        //所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的

                        //所以是否将搜索词点击品类id拼接到字符串中去
                        //首先要满足不能是null值
                        //其次，之前的字符串中还没有搜索词或者点击品类id

                        if(StringUtils.isNotEmpty(searchKeyword)) {
                            if(!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                                searchKeywordsBuffer.append(searchKeyword + ",");
                            }
                        }
                        if(clickCategoryId != null) {
                            if(!clickCategoryIdsBuffer.toString().contains(
                                    String.valueOf(clickCategoryId))) {
                                clickCategoryIdsBuffer.append(clickCategoryId + ",");
                            }
                        }

                        //计算session开始和结束时间
                        //现在DateUtils.java中添加方法
                        //public static Date parseTime(String time) {
                        //  try {
                        //      return TIME_FORMAT.parse(time);
                        //  } catch (ParseException e) {
                        //      e.printStackTrace();
                        //  }
                        //  return null;
                        //}

                        //计算session开始和结束时间
                        Date actionTime = DateUtils.parseTime(row.getString(4));
                        if(startTime == null) {
                            startTime = actionTime;
                        }
                        if(endTime == null) {
                            endTime = actionTime;
                        }

                        if(actionTime.before(startTime)) {
                            startTime = actionTime;
                        }
                        if(actionTime.after(endTime)) {
                            endTime = actionTime;
                        }

                        //计算session访问步长
                        stepLength ++;
                    }

                    //StringUtils引入的包是import com.wolf.sparkproject.util.trimComma;
                    String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                    String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                    //计算session访问时长（秒）
                    long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

                    //返回的数据即是<sessionid, partAggrInfo>
                    //但是，这一步聚合后，其实还需要将每一行数据，根对应的用户信息进行聚合
                    //问题来了，如果是跟用户信息进行聚合的话，那么key就不应该是sessionid，而应该是userid
                    //才能够跟<userid, Row>格式的用户信息进行聚合
                    //如果我们这里直接返回<sessionid, partAggrInfo>,还得再做一次mapToPair算子
                    //将RDD映射成<userid,partAggrInfo>的格式，那么就多此一举

                    //所以，我们这里其实可以直接返回数据格式就是<userid, partAggrInfo>
                    //然后在直接将返回的Tuple的key设置成sessionid
                    //最后的数据格式，还是<sessionid,fullAggrInfo>

                    //聚合数据，用什么样的格式进行拼接？
                    //我们这里统一定义，使用key=value|key=value

                    //在Constants.java中定义spark作业相关的常量
                    //String FIELD_SESSION_ID = "sessionid";
                    //String FIELD_SEARCH_KEYWORDS = "searchKeywords";
                    //String FIELD_CLICK_CATEGORY_IDS = "clickCategoryIds";
                    //String FIELD_VISIT_LENGTH = "visitLength";
                    //String FIELD_STEP_LENGTH = "stepLength";
                    //String FIELD_START_TIME = "starttime";
                    String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
                            + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                            + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                            + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                            + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                            + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);

                    return new Tuple2<Long, String>(userid, partAggrInfo);
                }
            });

        //查询所有用户数据
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();

        JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(
            new PairFunction<Row, Long, Row>(){

                private static final long serialVersionUID = 1L;

                @Override
                public Tuple2<Long, Row> call(Row row) throws Exception {
                    return new Tuple2<Long, Row>(row.getLong(0), row);
                }
            });

        //将session粒度聚合数据，与用户信息进行join
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD =
                userid2PartAggrInfoRDD.join(userid2InfoRDD);

        //对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(
            new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {

                private static final long serialVersionUID = 1L;

                @Override
                public Tuple2<String, String> call(
                        Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
                    String partAggrInfo = tuple._2._1;
                    Row userInfoRow = tuple._2._2;

                    String sessionid = StringUtils.getFieldFromConcatString(
                            partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

                    int age = userInfoRow.getInt(3);
                    String professional = userInfoRow.getString(4);
                    String city = userInfoRow.getString(5);
                    String sex = userInfoRow.getString(6);

                    //在Constants.java中添加以下常量
                    //String FIELD_AGE = "age";
                    //String FIELD_PROFESSIONAL = "professional";
                    //String FIELD_CITY = "city";
                    //String FIELD_SEX = "sex";
                    String fullAggrInfo = partAggrInfo + "|"
                            + Constants.FIELD_AGE + "=" + age + "|"
                            + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                            + Constants.FIELD_CITY + "=" + city + "|"
                            + Constants.FIELD_SEX + "=" + sex ;
                    return new Tuple2<String, String>(sessionid, fullAggrInfo);
                }
            });
        return sessionid2FullAggrInfoRDD;
    }

    /**
     * 过滤session数据，并进行聚合统计
     * @param sessionid2AggrInfoRDD
     * @return
     */
    private static JavaPairRDD<String, String> filterSessionAndAggrStat(
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
            final JSONObject taskParam,
            final Accumulator<String> sessionAggrAccumulator) {
        //为了使用后面的ValieUtils,所以，首先将所有的筛选参数拼接成一个连接串
        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");

        if (_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

        final String parameter = _parameter;

        //根据筛选参数进行过滤
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(

            new Function<Tuple2<String, String>, Boolean>() {

                private static final long serialVersionUID = 1L;

                public Boolean call(Tuple2<String, String> tuple) throws Exception {
                    //首先，从tuple中，获取聚合数据
                    String aggrInfo = tuple._2;

                    //接着，依次按照筛选条件进行过滤
                    //按照年龄范围进行过滤（startAge、endAge）
                    //先在Constants.java中添加常量
                    //String PARAM_START_AGE = "startAge";
                    //String PARAM_END_AGE = "endage";
                    //String PARAM_PROFESSIONALS = "professionals";
                    //String PARAM_CITIES = "cities";
                    //String PARAM_SEX = "sex";
                    //String PARAM_KEYWORDS = "keywords";
                    //String PARAM_CATEGORY_IDS = "categoryIds";
                    if(!ValidUtils.between(aggrInfo, Constants.FIELD_AGE,
                            parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                        return false;
                    }

                    //按照职业范围进行过滤（professionals）
                    if(!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL,
                            parameter, Constants.PARAM_PROFESSIONALS)) {
                        return false;
                    }

                    //按照城市范围进行过滤（cities）
                    if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY,
                            parameter, Constants.PARAM_CATEGORY_IDS)) {
                        return false;
                    }

                    //按照性别过滤
                    if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
                            parameter, Constants.PARAM_SEX)) {
                        return false;
                    }

                    //按照搜索词过滤
                    if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
                            parameter, Constants.PARAM_KEYWORDS)) {
                        return false;
                    }

                    //按照点击品类id进行搜索
                    if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS,
                            parameter, Constants.PARAM_CATEGORY_IDS)) {
                        return false;
                    }

                    //如果经过了之前的多个过滤条件之后，程序能够走到这里
                    //那么说明该session是通过了用户指定的筛选条件的，也就是需要保留的session
                    //那么就要对session的访问时长和访问步长进行统计，
                    //根据session对应的范围进行相应的累加计数
                    //只要走到这一步，那么就是需要计数的session
                    sessionAggrAccumulator.add(Constants.SESSION_COUNT);

                    //计算出session的访问时长和访问步长的范围，并进行相应的累加
                    long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                            aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                    long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                            aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
                    calculateVisitLength(visitLength);
                    calculateStepLength(stepLength);

                    return true;
                }

                /**
                 * 计算访问时长范围
                 * @param visitLength
                 */
                private void calculateVisitLength(long visitLength) {
                    if(visitLength >= 1 && visitLength <= 3) {
                        sessionAggrAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                    }else if(visitLength >= 4 && visitLength <= 6) {
                        sessionAggrAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                    }else if(visitLength >= 7 && visitLength <= 9) {
                        sessionAggrAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                    }else if(visitLength >= 10 && visitLength <= 30) {
                        sessionAggrAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                    }else if(visitLength > 30 && visitLength <= 60) {
                        sessionAggrAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                    }else if(visitLength > 60 && visitLength <= 180) {
                        sessionAggrAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                    }else if(visitLength > 180 && visitLength <= 600) {
                        sessionAggrAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                    }else if(visitLength > 600 && visitLength <= 1800) {
                        sessionAggrAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                    }else if(visitLength > 1800) {
                        sessionAggrAccumulator.add(Constants.TIME_PERIOD_30m);
                    }
                }

                /**
                 * 计算访问步长范围
                 * @param stepLength
                 */
                private void calculateStepLength(long stepLength) {
                    if(stepLength >= 1 && stepLength <= 3) {
                        sessionAggrAccumulator.add(Constants.STEP_PERIOD_1_3);
                    }else if(stepLength >= 4 && stepLength <= 6) {
                        sessionAggrAccumulator.add(Constants.STEP_PERIOD_4_6);
                    }else if(stepLength >= 7 && stepLength <= 9) {
                        sessionAggrAccumulator.add(Constants.STEP_PERIOD_7_9);
                    }else if(stepLength >= 10 && stepLength <= 30) {
                        sessionAggrAccumulator.add(Constants.STEP_PERIOD_10_30);
                    }else if(stepLength > 30 && stepLength <= 60) {
                        sessionAggrAccumulator.add(Constants.STEP_PERIOD_30_60);
                    }else if(stepLength > 60) {
                        sessionAggrAccumulator.add(Constants.STEP_PERIOD_60);
                    }
                }
            });
        return filteredSessionid2AggrInfoRDD;
    }

    /**
     * 获取通过筛选条件的session的访问明细数据RDD
     * @param sessionid2aggrInfoRDD
     * @param sessionid2actionRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getSessionid2detailRDD(
            JavaPairRDD<String, String> sessionid2aggrInfoRDD,
            JavaPairRDD<String, Row> sessionid2actionRDD) {
        JavaPairRDD<String, Row> sessionid2detailRDD = sessionid2aggrInfoRDD
                .join(sessionid2actionRDD)
                .mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {

                    private static final long serialVersionUID = 1L;

                    public Tuple2<String, Row> call(
                            Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                        return new Tuple2<String, Row>(tuple._1, tuple._2._2);
                    }

                });
        return sessionid2detailRDD;
    }

    /**
     * 随机抽取session
     * @param sc
     * @param taskid
     * @param sessionid2AggrInfoRDD
     * @param sessionid2actionRDD
     */
    private static void randomExtractSession(
            JavaSparkContext sc,
            final long taskid,
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
            JavaPairRDD<String, Row> sessionid2actionRDD) {

        /**
         * 第一步，计算每天每小时的session数量，
         */

        //获取<yyyy-mm-dd_hh,session>格式的RDD
        JavaPairRDD<String, String> time2sessionidRDD = sessionid2AggrInfoRDD.mapToPair(
            new PairFunction<Tuple2<String, String>, String, String>(){

                private static final long serialVersionUID = 1L;

                public Tuple2<String, String> call(
                        Tuple2<String, String> tuple) throws Exception {
                    String aggrInfo = tuple._2;
                    String startTime = StringUtils.getFieldFromConcatString(
                            aggrInfo, "\\|", Constants.FIELD_START_TIME);
                    String dateHour = DateUtils.getDateHour(startTime);
                    return new Tuple2<String, String>(dateHour, aggrInfo);
                }
            });

        //得到每天每小时的session数量
        Map<String, Object> countMap = time2sessionidRDD.countByKey();

        /**
         * 第二步，使用按时间比例随机抽取算法，计算出每小时要抽取session的索引
         */

        //将<yyyy-mm-dd_hh,count>格式的map转换成<yyyy-mm-dd,<hh,count>>的格式，方便后面使用
        Map<String, Map<String, Long>> dateHourCountMap =
                new HashMap<String, Map<String, Long>>();
        for(Map.Entry<String, Object>countEntry : countMap.entrySet()) {
            String dateHour = countEntry.getKey();
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];

            long count = Long.valueOf(String.valueOf(countEntry.getValue()));

            Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            if(hourCountMap == null) {
                hourCountMap = new HashMap<String, Long>();
                dateHourCountMap.put(date, hourCountMap);
            }

            hourCountMap.put(hour, count);
        }

        //开始实现按照时间比例随机抽取算法

        //总共抽取100个session，先按照天数平分
        int extractNumberPerDay = 100 / dateHourCountMap.size();

        //每一天每一个小时抽取session的索引，<date,<hour,(3,5,20,200)>>
        Map<String, Map<String, List<Integer>>> dateHourExtractMap =
                new HashMap<String, Map<String, List<Integer>>>();

        /**
         * 性能调优之广播大变量
         *
         * session 随机抽取功能使用到了随机抽取索引map这一比较大的变量，
         * 之前是直接在算子里使用了这个map，每个task都会拷贝一份map副本，
         * 比较消耗内存和网络传输性能，现在将其改为广播变量。
         */
        final Broadcast<Map<String, Map<String, List<Integer>>>> dateHourExtractMapBroadcast =
                sc.broadcast(dateHourExtractMap);

        Random random = new Random();

        for(Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
            String date = dateHourCountEntry.getKey();
            Map<String, Long> hourCountMap = dateHourCountEntry.getValue();

            //计算出这一天的session总数
            long sessionCount = 0L;
            for (long hourCount : hourCountMap.values()) {
                sessionCount += hourCount;
            }

            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if (hourExtractMap == null) {
                hourExtractMap = new HashMap<String, List<Integer>>();
                dateHourExtractMap.put(date, hourExtractMap);
            }

            //遍历每个小时
            for (Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();

                //计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量
                //就可以算出当前小时所需抽取的session数量
                long hourExtractNumber = (int) (((double) count / (double) sessionCount)
                        * extractNumberPerDay);

                if (hourExtractNumber > count) {
                    hourExtractNumber = (int) count;
                }

                //先获取当前小时的存放随机数list
                List<Integer> extractIndexList = hourExtractMap.get(hour);
                if (extractIndexList == null) {
                    extractIndexList = new ArrayList<Integer>();
                    hourExtractMap.put(hour, extractIndexList);
                }

                //生成上面计算出来的数量的随机数
                for (int i = 0; i < hourExtractNumber; i++) {
                    int extractIndex = random.nextInt((int) count);

                    //生成不重复的索引
                    while (extractIndexList.contains(extractIndex)) {
                        extractIndex = random.nextInt((int) count);
                    }

                    extractIndexList.add(extractIndex);
                }
            }
        }

        /**
         * 第三步：遍历每天每小时的session，根据随机索引抽取
         */

        //执行groupByKey算子，得到<dateHour,(session aggrInfo)>
        JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2sessionidRDD.groupByKey();

        //我们用flatMap算子遍历所有的<dateHour,(session aggrInfo)>格式的数据
        //然后会遍历每天每小时的session
        //如果发现某个session恰巧在我们指定的这天这小时的随机抽取索引上
        //那么抽取该session，直接写入MySQL的random_extract_session表
        //将抽取出来的session id返回回来，形成一个新的JavaRDD<String>
        //然后最后一步，用抽取出来的sessionid去join它们的访问行为明细数据写入session表
        JavaPairRDD<String, String> extractSessionidsRDD = time2sessionsRDD.flatMapToPair(

                new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {

                    private static final long serialVersionUID = 1L;

                    public Iterable<Tuple2<String, String>> call(
                            Tuple2<String, Iterable<String>> tuple)
                            throws Exception {
                        List<Tuple2<String, String>> extractSessionids =
                                new ArrayList<Tuple2<String, String>>();

                        String dateHour = tuple._1;
                        String date = dateHour.split("_")[0];
                        String hour = dateHour.split("_")[1];
                        Iterator<String> iterator = tuple._2.iterator();

                        Map<String, Map<String, List<Integer>>> dateHourExtractMap =
                                dateHourExtractMapBroadcast.value();

                        //拿到这一天这一小时的随机索引
                        List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);

                        //先建domain和DAO
                        //先在包com.wolf.sparkproject.domain中新建SessionRandomExtract.java
                        //然后在包com.wolf.sparkproject.dao中新建ISessionRandomExtractDAO.java
                        //接着在包com.wolf.sparkproject.impl中新建SessionRandomExtractDAOImpl.java
                        //最后在DAOFactory.java中添加
                        //public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
                        //return new SessioRandomExtractDAOImpl();
                        //}
                        ISessionRandomExtractDAO sessionRandomExtractDAO =
                                DAOFactory.getSessionRandomExtractDAO();

                        int index = 0;
                        while(iterator.hasNext()) {
                            String sessionAggrInfo = iterator.next();

                            if(extractIndexList.contains(index)) {
                                String sessionid = StringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

                                //将数据写入MySQL
                                SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();

                                //增加参数
                                //private static void randomExtractSession(
                                //final long taskid,
                                //JavaPairRDD<String, String> sessionid2AggrInfoRDD)
                                sessionRandomExtract.setTaskid(taskid);
                                sessionRandomExtract.setSessionid(StringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID));
                                sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_START_TIME));
                                sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
                                sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));

                                sessionRandomExtractDAO.insert(sessionRandomExtract);

                                //将sessionid加入list
                                extractSessionids.add(new Tuple2<String, String>(sessionid, sessionid));
                            }
                            index ++;
                        }
                        return extractSessionids;
                    }
                });

        /**
         * 第四步：获取抽取出来的session的明细数据
         */
        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD =
                extractSessionidsRDD.join(sessionid2actionRDD);
        extractSessionDetailRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {

            private static final long serialVersionUID = 1L;

            public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                //在包com.wolf.sparkproject.domain中新建SessionDetail.java
                //在包com.wolf.sparkproject.dao中新建ISessionDetailDAO.java接口
                //在包com.wolf.sparkproject.impl中新建SessionDetailDAOImpl.java
                //在DAOFactory.java中添加
                //public static ISessionDetailDAO getSessionDetailDAO() {
                //return new SessionDetailDAOImpl();
                //}
                Row row = tuple._2._2;

                //封装sessionDetail的domain
                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskid(taskid);
                sessionDetail.setUserid(row.getLong(1));
                sessionDetail.setSessionid(row.getString(2));
                sessionDetail.setPageid(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(row.getLong(6));
                sessionDetail.setClickProductId(row.getLong(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));

                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                sessionDetailDAO.insert(sessionDetail);
            }
        });
    }

    /**
     * 计算各session范围占比，并写入MySQL
     * @param value
     * @param taskid
     */
    private static void calculateAndPersistAggrStat(String value, long taskid) {
        //从Accumulator统计串中获取值
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.SESSION_COUNT));

        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_60));

        //计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double)visit_length_1s_3s / (double)session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double)visit_length_4s_6s / (double)session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double)visit_length_7s_9s / (double)session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double)visit_length_10s_30s / (double)session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double)visit_length_30s_60s / (double)session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double)visit_length_1m_3m / (double)session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double)visit_length_3m_10m / (double)session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_10m_30m / (double)session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_30m / (double)session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double)step_length_1_3 / (double)session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double)step_length_4_6 / (double)session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double)step_length_7_9 / (double)session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double)step_length_10_30 / (double)session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double)step_length_30_60 / (double)session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double)step_length_60 / (double)session_count, 2);

        //将访问结果封装成Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);

        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        //调用对用的DAO插入统计结果
        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);
    }

    /**
     * 获取Top10热门品类
     * @param taskid
     * @param sessionid2detailRDD
     */
    private static List<Tuple2<CategorySortKey, String>> getTop10Category(
            long taskid,
            JavaPairRDD<String, Row> sessionid2detailRDD) {

        /**
         * 第一步：获取符合条件的session访问过的所有品类
         */

        //获取session访问过的所有品类id
        //访问过指的是点击、下单、支付的品类
        JavaPairRDD<Long, Long> categoryidRDD = sessionid2detailRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    public Iterable<Tuple2<Long, Long>> call(
                            Tuple2<String, Row> tuple) throws Exception {

                        Row row = tuple._2;

                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                        Long clickCategoryId = row.getLong(6);
                        if (clickCategoryId != null) {
                            list.add(new Tuple2<Long, Long>(clickCategoryId, clickCategoryId));
                        }

                        String orderCategoryIds = row.getString(8);
                        if (orderCategoryIds != null) {
                            String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
                            for (String orderCategoryId : orderCategoryIdsSplited) {
                                list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId),
                                        Long.valueOf(orderCategoryId)));
                            }
                        }

                        String payCategoryIds = row.getString(10);
                        if (payCategoryIds != null) {
                            String[] payCategoryIdsSplited = payCategoryIds.split(",");
                            for (String payCategoryId : payCategoryIdsSplited) {
                                list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId),
                                        Long.valueOf(payCategoryId)));
                            }
                        }

                        return list;
                    }
                });

        //必须去重
        //如果不去重的话，会出现重复的categoryid，排序会对重复的categoryid以及countInfo进行排序
        //最后很可能会拿到重复的数据
        categoryidRDD = categoryidRDD.distinct();

        /**
         * 第二步：计算各品类的点击、下单和支付的次数
         */

        //访问明细中，其中三种访问行为是点击、下单和支付
        //分别来计算各品类点击、下单和支付的次数，可以先对访问明细数据进行过滤
        //分别过滤点击、下单和支付行为，然后通过map、reduceByKey等算子进行计算

        //计算各个品类点击次数
        JavaPairRDD<Long, Long> clickCategoryId2CountRDD =
                getClickCategoryId2CountRDD(sessionid2detailRDD);

        //计算各个品类的下单次数
        JavaPairRDD<Long, Long> orderCategoryId2CountRDD =
                getOrderCategoryId2CountRDD(sessionid2detailRDD);

        //计算各个品类的支付次数
        JavaPairRDD<Long, Long> payCategoryId2CountRDD =
                getPayCategoryId2CountRDD(sessionid2detailRDD);

        /**
         * 第三步：join各品类与它的点击、下单和支付的次数
         *
         * caetoryidRDD中包含了所有符合条件的session访问过的品类id
         *
         * 上面分别计算出来的三份各品类的点击、下单和支付的次数，可能不是包含所有品类的，
         * 比如，有的品类就只是被点击过，但是没有人下单和支付
         * 所以，这里就不能使用join操作，而是要使用leftOutJoin操作，
         * 就是说，如果categoryidRDD不能join到自己的某个数据，比如点击、下单或支付数据，
         * 那么该categoryidRDD还是要保留下来的
         * 只不过，没有join到的那个数据就是0了
         */
        JavaPairRDD<Long, String> categoryid2countRDD = joinCategoryAndData(
                categoryidRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD,
                payCategoryId2CountRDD);

        /**
         * 第四步：自定义二次排序key
         * 在com.wolf.sparkproject.spark.session包下创建CategorySortKey类
         */

        /**
         * 第五步：将数据映射成<SortKey,info>格式的RDD，然后进行二次排序（降序）
         */
        JavaPairRDD<CategorySortKey, String> sortKey2countRDD = categoryid2countRDD.mapToPair(
                new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {

                    private static final long serialVersionUID = 1L;

                    public Tuple2<CategorySortKey, String> call(
                            Tuple2<Long, String> tuple) throws Exception {
                        String countInfo = tuple._2;
                        long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                                countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
                        long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                                countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
                        long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                                countInfo, "\\|", Constants.FIELD_PAY_COUNT));

                        CategorySortKey sortKey = new CategorySortKey(clickCount,
                                orderCount, payCount);
                        return new Tuple2<CategorySortKey, String>(sortKey, countInfo);
                    }
                });

        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD =
                sortKey2countRDD.sortByKey(false);

        /**
         * 第六步：用take(10)取出top10热门品类，并写入MySQL
         */
        ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();

        List<Tuple2<CategorySortKey, String>> top10CategoryList =
                sortedCategoryCountRDD.take(10);
        for(Tuple2<CategorySortKey, String> tuple : top10CategoryList) {
            String countInfo = tuple._2;
            long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_CATEGORY_ID));
            long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
            long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
            long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_PAY_COUNT));

            //封装domain对象
            Top10Category category = new Top10Category();
            category.setTaskid(taskid);
            category.setCategoryid(categoryid);
            category.setClickCount(clickCount);
            category.setOrderCount(orderCount);
            category.setPayCount(payCount);

            top10CategoryDAO.insert(category);
        }
        return top10CategoryList;
    }

    /**
     * 获取各品类点击次数RDD
     */
    private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(
            JavaPairRDD<String, Row> sessionid2detailRDD) {
        JavaPairRDD<String, Row> clickActionRDD = sessionid2detailRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;

                        return row.get(6) != null ? true : false;
                    }
                });

        JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(
                new PairFunction<Tuple2<String, Row>, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    public Tuple2<Long, Long> call(Tuple2<String, Row> tuple)
                            throws Exception {
                        long clickCategoryId = tuple._2.getLong(6);

                        return new Tuple2<Long, Long>(clickCategoryId, 1L);
                    }
                });

        JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(

                new Function2<Long, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });
        return clickCategoryId2CountRDD;
    }

    /**
     * 计算各个品类的下单次数
     */
    private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(
            JavaPairRDD<String, Row>sessionid2detailRDD) {
        JavaPairRDD<String, Row> orderActionRDD = sessionid2detailRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        return row.getString(8) != null ? true : false;
                    }
                });

        JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>(){

                    private static final long serialVersionUID = 1L;

                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple)
                            throws Exception {

                        Row row = tuple._2;
                        String orderCategoryIds = row.getString(8);
                        String[] orderCategoryIdsSplited = orderCategoryIds.split(",");

                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                        for(String orderCategoryId : orderCategoryIdsSplited) {
                            list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 1L));
                        }

                        return list;
                    }
                });

        JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey(
                new Function2<Long, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }

                });
        return orderCategoryId2CountRDD;
    }

    /**
     * 计算各个品类的支付次数
     */
    private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(
            JavaPairRDD<String, Row> sessionid2detailRDD) {
        JavaPairRDD<String, Row> payActionRDD = sessionid2detailRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        return row.getString(10) != null ? true : false;
                    }
                });

        JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>(){

                    private static final long serialVersionUID = 1L;

                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple)
                            throws Exception {

                        Row row = tuple._2;
                        String payCategoryIds = row.getString(10);
                        String[] payCategoryIdsSplited = payCategoryIds.split(",");

                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                        for(String payCategoryId : payCategoryIdsSplited) {
                            list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), 1L));
                        }
                        return list;
                    }
                });

        JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey(
                new Function2<Long, Long, Long>(){

                    private static final long serialVersionUID = 1L;

                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });
        return payCategoryId2CountRDD;
    }

    private static JavaPairRDD<Long, String> joinCategoryAndData(
            JavaPairRDD<Long, Long> categoryidRDD,
            JavaPairRDD<Long, Long> clickCategoryId2CountRDD,
            JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
            JavaPairRDD<Long, Long> payCategoryId2CountRDD) {

        //如果使用leftOuterJoin就有可能出现右边那个RDD中join过来时没有值
        //所以Tuple2中的第二个值用Optional<Long>类型就代表可能有值也可能没有值
        JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinRDD =
                categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD);

        JavaPairRDD<Long, String> tmpMapRDD = tmpJoinRDD.mapToPair(

                new PairFunction<Tuple2<Long,Tuple2<Long,Optional<Long>>>, Long, String>() {

                    private static final long serialVersionUID = 1L;

                    public Tuple2<Long, String> call(
                            Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple)
                            throws Exception {
                        long categoryid = tuple._1;
                        Optional<Long> optional = tuple._2._2;
                        long clickCount = 0L;

                        if(optional.isPresent()) {
                            clickCount = optional.get();
                        }

                        String value = Constants.FIELD_CATEGORY_ID + "=" + categoryid + "|" +
                                Constants.FIELD_CLICK_COUNT + "=" + clickCount;

                        return new Tuple2<Long, String>(categoryid, value);
                    }

                });

        tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(

                new PairFunction<Tuple2<Long,Tuple2<String,Optional<Long>>>, Long, String>() {

                    private static final long serialVersionUID = 1L;

                    public Tuple2<Long, String> call(
                            Tuple2<Long, Tuple2<String, Optional<Long>>> tuple)
                            throws Exception {
                        long categoryid = tuple._1;
                        String value = tuple._2._1;

                        Optional<Long> optional = tuple._2._2;
                        long orderCount = 0L;

                        if(optional.isPresent()) {
                            orderCount = optional.get();
                        }

                        value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;

                        return new Tuple2<Long, String>(categoryid, value);
                    }

                });

        tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(

                new PairFunction<Tuple2<Long,Tuple2<String,Optional<Long>>>, Long, String>() {

                    private static final long serialVersionUID = 1L;


                    public Tuple2<Long, String> call(
                            Tuple2<Long, Tuple2<String, Optional<Long>>> tuple)
                            throws Exception {
                        long categoryid = tuple._1;
                        String value = tuple._2._1;

                        Optional<Long> optional = tuple._2._2;
                        long payCount = 0L;

                        if(optional.isPresent()) {
                            payCount = optional.get();
                        }

                        value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;

                        return new Tuple2<Long, String>(categoryid, value);
                    }

                });
        return tmpMapRDD;
    }

    /**
     * 获取top10活跃session
     * @param sc
     * @param taskid
     * @param top10CategoryList
     * @param sessionid2detailRDD
     */
    private static void getTop10Session(
            JavaSparkContext sc,
            final long taskid,
            List<Tuple2<CategorySortKey, String>> top10CategoryList,
            JavaPairRDD<String, Row> sessionid2detailRDD) {

        /**
         * 第一步：将top10热门品类的id生成一份RDD
         */

        //处理List
        List<Tuple2<Long, Long>> top10CategoryIdList =
                new ArrayList<Tuple2<Long, Long>>();

        for (Tuple2<CategorySortKey, String> category : top10CategoryList) {
            long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
                    category._2, "\\|", Constants.FIELD_CATEGORY_ID));
            top10CategoryIdList.add(new Tuple2<Long, Long>(categoryid, categoryid));
        }

        JavaPairRDD<Long, Long> top10CategoryIdRDD =
                sc.parallelizePairs(top10CategoryIdList);

        /**
         * 第二步：计算top10品类被各session点击的次数
         */
        JavaPairRDD<String, Iterable<Row>> sessionid2detailsRDD =
                sessionid2detailRDD.groupByKey();

        JavaPairRDD<Long, String> categoryid2sessionCountRDD = sessionid2detailsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {

                    private static final long serialVersionUID = 1L;

                    public Iterable<Tuple2<Long, String>> call(
                            Tuple2<String, Iterable<Row>> tuple) throws Exception {

                        String sessionid = tuple._1;
                        Iterator<Row> iterator = tuple._2.iterator();

                        Map<Long, Long> categoryCountMap = new HashMap<Long, Long>();

                        //计算出该session对每个品类的点击次数
                        while(iterator.hasNext()) {
                            Row row = iterator.next();

                            if(row.get(6) != null) {
                                long categoryid = row.getLong(6);

                                Long count = categoryCountMap.get(categoryid);
                                if(count == null) {
                                    count = 0L;
                                }
                                count ++;

                                categoryCountMap.put(categoryid, count);
                            }
                        }

                        //返回结果为<categoryid,session:count>格式
                        List<Tuple2<Long, String>> list = new ArrayList<Tuple2<Long, String>>();
                        for(Map.Entry<Long, Long> categoryCountEntry : categoryCountMap.entrySet()) {
                            long categoryid = categoryCountEntry.getKey();
                            long count = categoryCountEntry.getValue();
                            String value = sessionid + "," + count;
                            list.add(new Tuple2<Long, String>(categoryid, value));
                        }
                        return list;
                    }
                });

        //获取到top10热门品类被各个session点击的次数
        JavaPairRDD<Long, String> top10CategorySessionCountRDD =
                top10CategoryIdRDD.join(categoryid2sessionCountRDD)
                        .mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, String>>, Long, String>() {

                            private static final long serialVersionUID = 1L;

                            public Tuple2<Long, String> call(
                                    Tuple2<Long, Tuple2<Long, String>> tuple) throws Exception {
                                return new Tuple2<Long, String>(tuple._1, tuple._2._2);
                            }
                        });

        /**
         * 第三步：分组取topN算法实现，获取每个品类的top10活跃用户
         */
        JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD =
                top10CategorySessionCountRDD.groupByKey();

        JavaPairRDD<String, String> top10SessionRDD = top10CategorySessionCountsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>() {

                    private static final long serialVersionUID = 1L;

                    public Iterable<Tuple2<String, String>> call(
                            Tuple2<Long, Iterable<String>> tuple) throws Exception {

                        long categoryid = tuple._1;
                        Iterator<String> iterator = tuple._2.iterator();

                        //定义取TopN的排序数组
                        String[] top10Sessions = new String[10];

                        while(iterator.hasNext()) {
                            String sessionCount = iterator.next();
                            long count = Long.valueOf(sessionCount.split(",")[1]);

                            //遍历排序数组
                            for(int i = 0; i < top10Sessions.length; i++) {
                                //如果当前位没有数据，直接将i位数据赋值为当前sessioncount
                                if(top10Sessions[i] == null) {
                                    top10Sessions[i] = sessionCount;
                                    break;
                                }else {
                                    long _count = Long.valueOf(top10Sessions[i].split(",")[1]);

                                    //如果sessionCount比i位的sessionCount大
                                    if(count > _count) {
                                        //从排序数组最后一位开始，到i位所有数据往后挪一位
                                        for(int j = 9; j > i; j--) {
                                            top10Sessions[j] = top10Sessions[j - 1];
                                        }
                                        //将i位赋值为sessionCount
                                        top10Sessions[i] = sessionCount;
                                        break;
                                    }

                                    //如果sessionCount比i位的sessionCount要小，继续外层for循环
                                }
                            }
                        }

                        //将数据写入MySQL表
                        List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();

                        for(String sessionCount : top10Sessions) {
                            if(sessionCount != null) {
                                String sessionid = sessionCount.split(",")[0];
                                long count = Long.valueOf(sessionCount.split(",")[1]);

                                //将top10session插入MySQL表
                                Top10Session top10Session = new Top10Session();
                                top10Session.setTaskid(taskid);
                                top10Session.setCategoryid(categoryid);
                                top10Session.setSessionid(sessionid);
                                top10Session.setClickCount(count);

                                ITop10SessionDAO top10SessionDAO = DAOFactory.getTop10SessionDAO();
                                top10SessionDAO.insert(top10Session);

                                list.add(new Tuple2<String, String>(sessionid, sessionid));
                            }
                        }

                        return list;
                    }
                });

        /**
         * 第四步：获取top10活跃session的明细数据，并写入MySQL
         */
        JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD =
                top10SessionRDD.join(sessionid2detailRDD);
        sessionDetailRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() {

            private static final long serialVersionUID = 1L;

            public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                Row row = tuple._2._2;

                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskid(taskid);
                sessionDetail.setUserid(row.getLong(1));
                sessionDetail.setSessionid(row.getString(2));
                sessionDetail.setPageid(row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(row.getLong(6));
                sessionDetail.setClickProductId(row.getLong(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));

                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                sessionDetailDAO.insert(sessionDetail);
            }
        });
    }
}
