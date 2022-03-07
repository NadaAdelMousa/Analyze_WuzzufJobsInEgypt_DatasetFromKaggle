package com.Team5.spring;


import java.awt.Color;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;

import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.CategorySeries;
import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;
import org.knowm.xchart.PieSeries;
import org.knowm.xchart.style.Styler;
import org.knowm.xchart.style.theme.GGPlot2Theme;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.MediaType;
import org.springframework.util.StreamUtils;

import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.linalg.Vector;


@Controller
public class AppController {
    @Autowired
    private SparkSession sparkSession;

    @RequestMapping("read-csv")
    public ResponseEntity<String> showData() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("src/main/resources/Wuzzuf_Jobs.csv");
        String topRows = dataset.showString(5, 0, true).replaceAll("only showing top \\d* rows", "");
        String [] Records = topRows.split("-RECORD");
        for (int i = 1;i<Records.length; i++){
            Records[i] = "<td>" + Records[i].substring(149).replaceAll("\\w*\\s*\\|", "</td> <td>") + "</td>";
        }
        topRows = "<table> <tr><th>Title</th><th>Company</th><th>Location</th><th>Type</th><th>Level</th><th>YearsExp</th><th>Country</th><th>Skills</th></tr> <tr>"
                + String.join("</tr> <tr>", Records) + "</tr> </table>";
        String html =
                String.format("<h2>Total records %d</h2>", dataset.count())
                        + topRows + "<br/>  <br/>" + String.format("<h4>only showing top %d rows</h4>", Records.length-1);
        html= "<head><style>table {font-family: arial, sans-serif;border-collapse: collapse;width: 90%;}td, th {border: 1px solid #dddddd;text-align: center;padding: 8px;}tr:nth-child(even) {background-color: #dddddd;}</style></head>" + html;
        return ResponseEntity.ok(html);
    }
    
    @RequestMapping("structure-and-summary")
    public ResponseEntity<String> showSummary() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("src/main/resources/Wuzzuf_Jobs.csv");
        String html = dataset.schema().treeString().replaceAll("root", "");
        html = html.replace("|--", "<br/>  <br/>");
        
        dataset.createOrReplaceTempView("ViewToWuzzafData");
        html = html + "<br/>  <br/>Duplicates_number : " + (dataset.count() - sparkSession.sql("select distinct * from ViewToWuzzafData ").count());
        Dataset<Row> WJ_1 = dataset.na().drop();
        Dataset<Row> WJ_final = WJ_1.dropDuplicates();
        html = html + String.format("<h4>Total records after dropping Nulls and Duplicates: %d</h4>", WJ_final.count());
        return ResponseEntity.ok(html);
    }
    
    @RequestMapping("demanding-companies")
    public ResponseEntity<String> ShowCompanies() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("src/main/resources/Wuzzuf_Jobs.csv");
        dataset = dataset.na().drop().dropDuplicates();
         
        dataset.createOrReplaceTempView("ViewToWuzzafData");
        Dataset<Row> companies = sparkSession.sql("select count(Title) as Jobs_per_company , Company from ViewToWuzzafData group by Company order by Jobs_per_company desc");
        String topRows = companies.showString((int) companies.count(), 0, true);
        
        String [] Records = topRows.split("-RECORD");
        for (int i = 1;i<Records.length; i++){
            Records[i] = Records[i].substring(85).replaceAll("\\w*\\s*\\|", "</td> <td>") + "</td>";
        }
        topRows = "<table> <tr><th>Number of vacancies</th><th>Company</th></tr> <tr>"
                + String.join("</tr> <tr>", Records) + "</tr> </table>";
        topRows= "<head><style>table {font-family: arial, sans-serif;border-collapse: collapse;width: 25%;}td, th {border: 1px solid #dddddd;text-align: center;padding: 8px;}tr:nth-child(even) {background-color: #dddddd;}</style></head>" + topRows;

        return ResponseEntity.ok(topRows);
    }
    
 
     public void DrawPieChart() throws IOException {
 
         PieChart chart = new PieChartBuilder().width(800).height(600).title("Jobs for each company percentage").build();
         Color[] sliceColors = new Color[]{ new Color(0, 255, 0), new Color(0, 0, 255),new Color(255, 0, 0), new Color(255, 255, 0),
         new Color(0, 255, 255), new Color(255, 0, 255), new Color(255, 111, 0),
         new Color(157, 255, 100, 255), new Color(125, 0, 250)};
         chart.addSeries("Confidential", 590);
         chart.addSeries("Mishkat Nour", 39);
         chart.addSeries("Expand Cart", 35);
         chart.addSeries("EGIC", 34);
         chart.addSeries("Aqarmap.com", 25);
         chart.addSeries("Majorel Egypt", 23);
         chart.addSeries("Ghassan Ahmed Als...", 21);
         chart.addSeries("Flairstech", 18);
         chart.addSeries("Profolio Consulting", 17);
         chart.addSeries("OPPO Egypt", 16);
         chart.getStyler().setHasAnnotations(true);
         chart.getStyler().setAnnotationDistance(1.22);
         chart.getStyler().setPlotContentSize(.7);
         chart.getStyler().setDefaultSeriesRenderStyle(PieSeries.PieSeriesRenderStyle.Donut);
         chart.getStyler().setToolTipsEnabled(true);
         chart.getStyler().setSeriesColors(sliceColors);
         
         BitmapEncoder.saveBitmap(chart,"src/main/resources/Companies_Jobs.png",BitmapEncoder.BitmapFormat.PNG);
   }
     
     @RequestMapping("displayPieChart") 
     public ResponseEntity<byte[]> DisplayPieChart()throws IOException {
         //DrawPieChart();
         ClassPathResource imageFile = new ClassPathResource("/Companies_Jobs.png");
         byte[] imageBytes = StreamUtils.copyToByteArray(imageFile.getInputStream());

       return ResponseEntity.ok().contentType(MediaType.IMAGE_PNG).body(imageBytes);
}
    
        
    @RequestMapping("job-titles")
    public ResponseEntity<String> ShowJobTitles() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("src/main/resources/Wuzzuf_Jobs.csv");
        dataset = dataset.na().drop().dropDuplicates();
         
        dataset.createOrReplaceTempView("ViewToWuzzafData");
        Dataset<Row> titles= sparkSession.sql("select count(Title) as number_of_jobs , Title from ViewToWuzzafData group by Title order by number_of_jobs desc");
        String topRows = titles.showString((int) titles.count(), 0, true);
        
        String [] Records = topRows.split("-RECORD");
        for (int i = 1;i<Records.length; i++){
            Records[i] = Records[i].substring(67).replaceAll("\\w*\\s*\\|", "</td> <td>") + "</td>";
        }
        topRows = "<table> <tr><th>Number of vacancies</th><th>Title</th></tr> <tr>"
                + String.join("</tr> <tr>", Records) + "</tr> </table>";
        topRows= "<head><style>table {font-family: arial, sans-serif;border-collapse: collapse;width: 25%;}td, th {border: 1px solid #dddddd;text-align: center;padding: 8px;}tr:nth-child(even) {background-color: #dddddd;}</style></head>" + topRows;

        return ResponseEntity.ok(topRows);
    }
     
      public void DrawBarChart1() throws IOException {
            
            Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("src/main/resources/Wuzzuf_Jobs.csv");
            dataset = dataset.na().drop().dropDuplicates();
            
            dataset.createOrReplaceTempView("ViewToWuzzafData");
            Dataset<Row> titles= sparkSession.sql("select Title , count(Title) as number_of_jobs from ViewToWuzzafData group by Title order by number_of_jobs desc limit 10");
            
            List<String> Titles_list = (titles.select("Title")).as(Encoders.STRING()).collectAsList();
            List<Long> Number_list = (titles.select("number_of_jobs")).as(Encoders.LONG()).collectAsList();
            
            CategoryChart chart_title = new CategoryChartBuilder().width(1024).height(1024).title("Number of Jobs").xAxisTitle("Titles").yAxisTitle("Count").build();
            chart_title.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
            chart_title.getStyler().setDefaultSeriesRenderStyle(CategorySeries.CategorySeriesRenderStyle.Bar);
            chart_title.getStyler().setHasAnnotations(true);
            chart_title.getStyler().setStacked(true);
            chart_title.getStyler().setTheme(new GGPlot2Theme());
            chart_title.addSeries("Number of jobs ",Titles_list, Number_list);
     
            BitmapEncoder.saveBitmap(chart_title,"src/main/resources/Number_of_Jobs.png",BitmapEncoder.BitmapFormat.PNG);
    }
     
     @RequestMapping("displayBarChart1") 
     public ResponseEntity<byte[]> DisplayBarChart1()throws IOException {
         //DrawBarChart1();
         ClassPathResource imageFile = new ClassPathResource("/Number_of_Jobs.png");
         byte[] imageBytes = StreamUtils.copyToByteArray(imageFile.getInputStream());

       return ResponseEntity.ok().contentType(MediaType.IMAGE_PNG).body(imageBytes);
}
     
    @RequestMapping("areas")
    public ResponseEntity<String> ShowAreas() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("src/main/resources/Wuzzuf_Jobs.csv");
        dataset = dataset.na().drop().dropDuplicates();
         
        dataset.createOrReplaceTempView("ViewToWuzzafData");
        Dataset<Row> loc = sparkSession.sql("select count(Location) as most_famous_locations , Location from ViewToWuzzafData group by Location order by most_famous_locations desc");

        String topRows = loc.showString((int) loc.count(), 0, true);
        
        String [] Records = topRows.split("-RECORD");
        for (int i = 1;i<Records.length; i++){
            Records[i] = Records[i].substring(41).replaceAll("\\w*\\s*\\|", "</td> <td>") + "</td>";
        }
        topRows = "<table> <tr><th>Number of repetition</th><th>Location</th></tr> <tr>"
                + String.join("</tr> <tr>", Records) + "</tr> </table>";
        topRows= "<head><style>table {font-family: arial, sans-serif;border-collapse: collapse;width: 25%;}td, th {border: 1px solid #dddddd;text-align: center;padding: 8px;}tr:nth-child(even) {background-color: #dddddd;}</style></head>" + topRows;

        return ResponseEntity.ok(topRows);
    }
    
    
     public void DrawBarChart2() throws IOException {
        
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("src/main/resources/Wuzzuf_Jobs.csv");
        dataset = dataset.na().drop().dropDuplicates();
         
        dataset.createOrReplaceTempView("ViewToWuzzafData");
        
        Dataset<Row> loc = sparkSession.sql("select count(Location) as most_famous_locations , Location from ViewToWuzzafData group by Location order by most_famous_locations desc limit 10");
        
        List<String> Locations = (loc.select("Location")).as(Encoders.STRING()).collectAsList();
        List<Long> Number_lst = (loc.select("most_famous_locations")).as(Encoders.LONG()).collectAsList();
        
        CategoryChart chart_area = new CategoryChartBuilder().width(1024).height(1024).title("Most Famous Area").xAxisTitle("Area").yAxisTitle("Count").build();
        chart_area.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart_area.getStyler().setDefaultSeriesRenderStyle(CategorySeries.CategorySeriesRenderStyle.Bar);
        chart_area.getStyler().setHasAnnotations(true);
        chart_area.getStyler().setStacked(true);
        chart_area.getStyler().setTheme(new GGPlot2Theme());
        chart_area.addSeries("Most Famous Areas",Locations, Number_lst);
   
        BitmapEncoder.saveBitmap(chart_area,"src/main/resources/Most_Famous_Areas.png",BitmapEncoder.BitmapFormat.PNG);
        
    }
     @RequestMapping("displayBarChart2") 
     public ResponseEntity<byte[]> DisplayBarChart2()throws IOException {
         //DrawBarChart2();
         ClassPathResource imageFile = new ClassPathResource("/Most_Famous_Areas.png");
         byte[] imageBytes = StreamUtils.copyToByteArray(imageFile.getInputStream());

       return ResponseEntity.ok().contentType(MediaType.IMAGE_PNG).body(imageBytes);
}
     
    @RequestMapping("skills")
    public ResponseEntity<String> ShowSkills() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("src/main/resources/Wuzzuf_Jobs.csv");
        dataset = dataset.na().drop().dropDuplicates();
         
        dataset.createOrReplaceTempView("ViewToWuzzafData");
        Dataset<Row> skills = sparkSession.sql("SELECT count(Skill) as Count_of_skill, Skill from (select explode(split(Skills, ',')) as Skill from ViewToWuzzafData ) as new_skill_col group by Skill order by Count_of_skill desc");

        String topRows = skills.showString((int) skills.count(), 0, true);
        
        String [] Records = topRows.split("-RECORD");
        for (int i = 1;i<Records.length; i++){
            Records[i] = Records[i].substring(77).replaceAll("\\w*\\s*\\|", "</td> <td>") + "</td>";
        }
        topRows = "<table> <tr><th>Number of being required</th><th>Skill</th></tr> <tr>"
                + String.join("</tr> <tr>", Records) + "</tr> </table>";
        topRows= "<head><style>table {font-family: arial, sans-serif;border-collapse: collapse;width: 25%;}td, th {border: 1px solid #dddddd;text-align: center;padding: 8px;}tr:nth-child(even) {background-color: #dddddd;}</style></head>" + topRows;

        return ResponseEntity.ok(topRows);
    }


    @RequestMapping("Factorized-YOE-Min")
    public ResponseEntity<String> ShowFactorizedYOE_Min() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("src/main/resources/Wuzzuf_Jobs.csv");
        dataset = dataset.na().drop().dropDuplicates();

        dataset.createOrReplaceTempView("ViewToWuzzafData");
        Dataset<Row> loc = sparkSession.sql("select *, case when YearsExp like '%+%' then cast(split(YearsExp, '[+] ')[0] as int) when YearsExp like '%-%' then cast(split(YearsExp, '-')[0] as int) else 0 end as YearsExpNumeric from ViewToWuzzafData");

        String topRows = loc.showString(10, 0, true).replaceAll("only showing top \\d* rows", "");

        String [] Records = topRows.split("-RECORD");
        for (int i = 1;i<Records.length; i++){
            Records[i] = Records[i].substring(146).replaceAll("\\w*\\s*\\|", "</td> <td>") + "</td>";
        }
        topRows = "<table> <tr><th>Title</th><th>Company</th><th>Location</th><th>Type</th><th>Level</th><th>YearsExp</th><th>Country</th><th>Skills</th><th>Factorized_YOE (Min)</th></tr> <tr>"
                + String.join("</tr> <tr>", Records) + "</tr> </table>";

        String html = String.format("<h2>Total records %d</h2>", dataset.count())
                        + topRows + "<br/>  <br/>" + String.format("<h4>only showing top %d rows</h4>", Records.length-1);
        html= "<head><style>table {font-family: arial, sans-serif;border-collapse: collapse;width: 100%;}td, th {border: 1px solid #dddddd;text-align: center;padding: 8px;}tr:nth-child(even) {background-color: #dddddd;}</style></head>" + html;
        return ResponseEntity.ok(html);
    }


    @RequestMapping("Factorized-YOE-One-Hot-encoding")
    public ResponseEntity<String> ShowFactorizedYOE_OneHotEncoding() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("src/main/resources/Wuzzuf_Jobs.csv");
        dataset = dataset.na().drop().dropDuplicates();
        StringIndexer indexer = new StringIndexer().setInputCol("YearsExp").setOutputCol("Factorized_YOE");
        String topRows = indexer.fit(dataset).transform(dataset).showString(10, 0, true).replaceAll("only showing top \\d* rows", "");
        String [] Records = topRows.split("-RECORD");
        for (int i = 1;i<Records.length; i++){
            Records[i] = "<td>" + Records[i].substring(164).replaceAll("\\w*\\s*\\|", "</td> <td>") + "</td>";
        }
        topRows = "<table> <tr><th>Title</th><th>Company</th><th>Location</th><th>Type</th><th>Level</th><th>YearsExp</th><th>Country</th><th>Skills</th><th>Factorized_YOE (One-Hot Encoding)</th></tr> <tr>"
                + String.join("</tr> <tr>", Records) + "</tr> </table>";
        String html =
                String.format("<h2>Total records %d</h2>", dataset.count())
                        + topRows + String.format("<h4>only showing top %d rows</h4>", Records.length-1);
        html= "<head><style>table {font-family: arial, sans-serif;border-collapse: collapse;width: 100%;}td, th {border: 1px solid #dddddd;text-align: center;padding: 8px;}tr:nth-child(even) {background-color: #dddddd;}</style></head>" + html;
        return ResponseEntity.ok(html);
    }


    @RequestMapping("Title-Company-K-means")
    public ResponseEntity<String> ShowTitleCompanyK_means() {
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("src/main/resources/Wuzzuf_Jobs.csv");
        dataset = dataset.na().drop().dropDuplicates();


        StringIndexer str = new StringIndexer()
                .setInputCol("Title")
                .setOutputCol("index1");

        StringIndexerModel mod = str.fit(dataset);
        Dataset<Row> index1 = mod.transform(dataset);



        StringIndexer str2 = new StringIndexer()
                .setInputCol("Company")
                .setOutputCol("index2");

        StringIndexerModel mod2 = str2.fit(dataset);
        Dataset<Row> index2 = mod2.transform(dataset);


        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[] {"index1"})
                .setOutputCol("features");

        Dataset<Row> features = assembler.transform(index1);
        KMeans kmeans = new KMeans().setK(2).setSeed(1L);
        KMeansModel model_2 = kmeans.fit(features);

        double WSSSE = model_2.computeCost(features);
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
        org.apache.spark.ml.linalg.Vector[] centers = model_2.clusterCenters();
        System.out.println("Cluster Centers: ");
        for (org.apache.spark.ml.linalg.Vector center: centers) {
            System.out.println(center);
        }

        VectorAssembler assembler2 = new VectorAssembler()
                .setInputCols(new String[] {"index2"})
                .setOutputCol("features");

        Dataset<Row> features2 = assembler2.transform(index2);
        KMeans kmeans_1 = new KMeans().setK(2).setSeed(1L);
        KMeansModel model_3 = kmeans.fit(features2);

        double WSSSE_2 = model_3.computeCost(features2);
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE_2);
        org.apache.spark.ml.linalg.Vector[] centers_2 = model_3.clusterCenters();
        System.out.println("Cluster Centers_2: ");
        for (Vector center: centers_2) {
            System.out.println(center);
        }
        String SE_Title= DecimalFormat.getNumberInstance().format(WSSSE);
        String clusterTitleCenter= centers.toString();
        String SE_Company= DecimalFormat.getNumberInstance().format(WSSSE_2);
        String clusterCompanyCenter= centers_2.toString();

        String html = "<table> <tr><th>SE_Title</th><th>clusterTitleCenter</th><th>SE_Company</th><th>clusterCompanyCenter</th></tr> <tr>"
                + SE_Title + clusterTitleCenter + SE_Company + clusterCompanyCenter + "</tr> </table>";
        html= "<head><style>table {font-family: arial, sans-serif;border-collapse: collapse;width: 100%;}td, th {border: 1px solid #dddddd;text-align: center;padding: 8px;}tr:nth-child(even) {background-color: #dddddd;}</style></head>" + html;

        return ResponseEntity.ok(html);

    }

}

