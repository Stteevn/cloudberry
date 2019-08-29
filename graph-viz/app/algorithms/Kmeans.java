package algorithms;

import models.Point;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;

/**
 * K-Means algorithm
 */
public class Kmeans {
    private int k; // the number of clusters desired
    private int m; // the number of iterations
    private int dataSetLength; // the number of points in the dataset
    private List<double[]> dataSet; // the dataset list
    private ArrayList<double[]> center; // the list of centers of clusters
    private List<List<double[]>> cluster; // the list of clusters for the whole dataset
    private ArrayList<Double> squaredErrorSums; // the sum of squared errors
    private Random random;
    private HashMap<Point, Integer> parents = new HashMap<>(); // map of points and its cluster

    public List<double[]> getDataSet() {
        return dataSet;
    }

    public HashMap<Point, Integer> getParents() {
        return parents;
    }

    public ArrayList<double[]> getCenter() {
        return center;
    }

    public int getDataSetLength() {
        return dataSetLength;
    }

    public int getK() {
        return k;
    }

    /**
     * Set the dataset for clustering
     *
     * @param dataSet
     */

    public void setDataSet(List<double[]> dataSet) {
        this.dataSet = dataSet;
    }

    /**
     * Get the resulting clusters
     *
     * @return Clustering result
     */

    public List<List<double[]>> getCluster() {
        return cluster;
    }

    /**
     * Constructor for k
     *
     * @param k Number of clusters
     */
    public Kmeans(int k) {
        if (k <= 0) {
            k = 1;
        }
        this.k = k;
    }

    /**
     * Initialization of the whole K-Means process
     */
    private void init() {
        m = 0;
        random = new Random();
        if (dataSet == null || dataSet.size() == 0) {
            initDataSet();
        }
        dataSetLength = dataSet.size();
        center = initCenters(dataSetLength, dataSet, random);
        cluster = initCluster();
        squaredErrorSums = new ArrayList<>();
    }

    /**
     * If the dataset is not initialized, use the one below for testing
     */
    private void initDataSet() {
        dataSet = new ArrayList<>();
        double[][] dataSetArray = new double[][]{{8, 2}, {3, 4}, {2, 5},
                {4, 2}, {7, 3}, {6, 2}, {4, 7}, {6, 3}, {5, 3},
                {6, 3}, {6, 9}, {1, 6}, {3, 9}, {4, 1}, {8, 6}};

        dataSet.addAll(Arrays.asList(dataSetArray));
    }

    /**
     * Initialize the list of centers corresponding to each cluster
     *
     * @return the list of centers
     */
    ArrayList<double[]> initCenters(int dataSetLength, List<double[]> dataSet, Random random) {
        ArrayList<double[]> center = new ArrayList<>();
        int[] randoms = new int[k];
        boolean flag;
        int temp = random.nextInt(dataSetLength);
        randoms[0] = temp;
        for (int i = 1; i < k; i++) {
            flag = true;
            while (flag) {
                temp = random.nextInt(dataSetLength);
                int j = 0;
                while (j < i) {
                    if (temp == randoms[j]) {
                        break;
                    }
                    j++;
                }
                if (j == i) {
                    flag = false;
                }
            }
            randoms[i] = temp;
        }
        for (int i = 0; i < k; i++) {
            center.add(dataSet.get(randoms[i]));
        }
        return center;
    }

    /**
     * Initialize the set of clusters
     *
     * @return a set of k empty clusters
     */
    List<List<double[]>> initCluster() {
        List<List<double[]>> cluster = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            cluster.add(new ArrayList<>());
        }

        return cluster;
    }

    /**
     * Calculate the distance between two points
     *
     * @param element points in dataset
     * @param center  centers of clusters
     * @return the computed distance
     */
    double distance(double[] element, double[] center) {
        double distance;
        double x = element[0] - center[0];
        double y = element[1] - center[1];
        double z = x * x + y * y;
        distance = Math.sqrt(z);

        return distance;
    }

    /**
     * Get the index of the center closest to a point
     *
     * @param distance distance array
     * @return the index of the closest center in the distance array
     */
    int minDistance(double[] distance, Random random) {
        double minDistance = distance[0];
        int minLocation = 0;
        for (int i = 1; i < distance.length; i++) {
            if (distance[i] < minDistance) {
                minDistance = distance[i];
                minLocation = i;
            } else if (distance[i] == minDistance) // 如果相等，随机返回一个位置
            {
                if (random.nextInt(10) < 5) {
                    minLocation = i;
                }
            }
        }
        return minLocation;
    }

    /**
     * Add each point to its closest cluster
     */
    private void clusterSet() {
        double[] distance = new double[k];
        for (int i = 0; i < dataSetLength; i++) {
            for (int j = 0; j < k; j++) {
                distance[j] = distance(dataSet.get(i), center.get(j));
            }
            int minLocation = minDistance(distance, random);
            cluster.get(minLocation).add(dataSet.get(i)); // add each point to its closest cluster
        }
    }

    /**
     * Map each point to the cluster it belongs to
     */
    private void findParents() {
        for (int i = 0; i < cluster.size(); i++) {
            for (int j = 0; j < cluster.get(i).size(); j++) {
                Point point = new Point(cluster.get(i).get(j)[0], cluster.get(i).get(j)[1]);
                parents.put(point, i);
            }
        }
    }

    /**
     * Calculate the squared error between two points
     *
     * @param element points in dataset
     * @param center  centers of clusters
     * @return the computed squared error
     */
    private double errorSquare(double[] element, double[] center) {
        double x = element[0] - center[0];
        double y = element[1] - center[1];

        return x * x + y * y;
    }

    /**
     * Calculate the sum of the squared error
     */
    private void countRule() {
        double squaredErrorSum = 0;
        for (int i = 0; i < cluster.size(); i++) {
            for (int j = 0; j < cluster.get(i).size(); j++) {
                squaredErrorSum += errorSquare(cluster.get(i).get(j), center.get(i));
            }
        }
        squaredErrorSums.add(squaredErrorSum);
    }

    /**
     * Set the new center for each cluster
     */
    private void setNewCenter() {
        for (int i = 0; i < k; i++) {
            int n = cluster.get(i).size();
            if (n != 0) {
                double[] newCenter = {0, 0};
                for (int j = 0; j < n; j++) {
                    newCenter[0] += cluster.get(i).get(j)[0];
                    newCenter[1] += cluster.get(i).get(j)[1];
                }
                // Calculate the average coordinate of all points in the cluster
                newCenter[0] = newCenter[0] / n;
                newCenter[1] = newCenter[1] / n;
                center.set(i, newCenter);
            }
        }
    }

    /**
     * Print data for testing
     *
     * @param dataArray     dataset
     * @param dataArrayName dataset name
     */
    public void printDataArray(ArrayList<double[]> dataArray,
                               String dataArrayName) {
        for (int i = 0; i < dataArray.size(); i++) {
            System.out.println("print:" + dataArrayName + "[" + i + "]={"
                    + dataArray.get(i)[0] + "," + dataArray.get(i)[1] + "}");
        }
        System.out.println("===================================");
    }

    /**
     * the core method of K-Means
     */
    public void kmeans() {
        init();
        // iterate until no change in the sum of squared errors
        while (true) {
            clusterSet();
            countRule();
            if (m != 0) {
                if (squaredErrorSums.get(m) - squaredErrorSums.get(m - 1) == 0) {
                    findParents();
                    break;
                }
            }
            setNewCenter();
            m++;
            cluster.clear();
            cluster = initCluster();
        }
        System.out.println("note:the times of repeat:m=" + m); // output the number of iterations
    }

    /**
     * execute the K-Means algorithm
     */
    public void execute() {
        long startTime = System.currentTimeMillis();
        System.out.println("kmeans begins");
        kmeans();
        long endTime = System.currentTimeMillis();
        System.out.println("kmeans running time=" + (endTime - startTime)
                + "ms");
        System.out.println("kmeans ends");
        System.out.println();
    }

    /**
     * Experiment setup
     *
     * @param args
     */
    public static void main(String[] args) {
        int k = 100;
        Kmeans kmeans = new Kmeans(k);
        ArrayList<double[]> dataSet = new ArrayList<>();
        int tot = 640000;
        for (int i = 0; i < tot; i++) {
            dataSet.add(new double[]{(double) Math.random(), (double) Math.random()});
        }
        long startTime = System.currentTimeMillis();
        kmeans.setDataSet(dataSet);
        kmeans.execute();
        long endTime = System.currentTimeMillis();
        NumberFormat formatter = new DecimalFormat("#0.00000");
        System.out.print("Execution time is " + formatter.format((endTime - startTime) / 1000d) + " seconds");
    }
}
