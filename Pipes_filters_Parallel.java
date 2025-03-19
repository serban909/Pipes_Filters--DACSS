import java.io.*;
import java.util.*;
import java.util.concurrent.*;

interface Filter extends Runnable
{
    
}

class ReaderFilter implements Filter
{
    private String inputFile;
    private BlockingQueue<String> outputQueue;

    public ReaderFilter(String inputFile, BlockingQueue<String> outputQueue) 
    {
        this.inputFile = inputFile;
        this.outputQueue = outputQueue;
    }

    public void run()
    {
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) 
        {
            String line;
            while ((line = reader.readLine()) != null) 
            {
                outputQueue.put(line);
            }
            outputQueue.put("STOP");
        }
        catch(IOException | InterruptedException e)
        {
            e.printStackTrace();
        }
    }
}

class BuyerFilter implements Filter 
{
    private HashSet<String> buyers;
    private BlockingQueue<String> inputQueue;
    private BlockingQueue<String> outputQueue;

    public BuyerFilter(HashSet<String> buyers, BlockingQueue<String> inputQueue, BlockingQueue<String> outputQueue) 
    {
        this.buyers = buyers;
        this.inputQueue = inputQueue;
        this.outputQueue = outputQueue;
    }

    public void run()
    {
        try
        {
            while(true)
            {
                String message=inputQueue.take();
                if(message.equals("STOP")) break;

                String[] words = message.split(", ");
                if(words.length >= 2 && buyers.contains(words[0].trim()+" - "+words[1].trim()))
                {
                    outputQueue.put(message);
                }
            }
            outputQueue.put("STOP");
        }
        catch(InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }
}

class ProfanityFilter implements Filter 
{
    private BlockingQueue<String> inputQueue;
    private BlockingQueue<String> outputQueue;

    public ProfanityFilter(BlockingQueue<String> inputQueue, BlockingQueue<String> outputQueue)
    {
        this.inputQueue = inputQueue;
        this.outputQueue = outputQueue;
    }

    public void run()
    {
        try
        {
            while(true)
            {
                String message=inputQueue.take();
                if(message.equals("STOP")) break;

                if(!message.contains("@#$%"))
                {
                    outputQueue.put(message);
                }
            }
            outputQueue.put("STOP");
        }
        catch(InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }
}

class PoliticalFilter implements Filter 
{
    private BlockingQueue<String> inputQueue;
    private BlockingQueue<String> outputQueue;

    public PoliticalFilter (BlockingQueue<String> inputQueue, BlockingQueue<String> outputQueue)
    {
        this.inputQueue = inputQueue;
        this.outputQueue=outputQueue;
    }

    public void run()
    {
        try
        {
            while(true)
            {
                String message=inputQueue.take();
                if(message.equals("STOP")) break;

                if(!message.contains("+++") && !message.contains("---"))
                {
                    outputQueue.put(message);
                }
            }
            outputQueue.put("STOP");
        }
        catch(InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }
}

class ImageResizer implements Filter 
{
    private BlockingQueue<String> inputQueue;
    private BlockingQueue<String> outputQueue;

    public ImageResizer (BlockingQueue<String> inputQueue, BlockingQueue<String> outputQueue)
    {
        this.inputQueue=inputQueue;
        this.outputQueue=outputQueue;
    }

    public void run()
    {
        try
        {
            while(true)
            {
                String message=inputQueue.take();
                if(message.equals("STOP")) break;

                String[] parts=message.split(", ");
                if(parts.length>=4)
                {
                    parts[3]=parts[3].toLowerCase();
                }
                outputQueue.put(String.join(", ", parts));
            }
            outputQueue.put("STOP");
        }
        catch(InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }
}

class LinkRemover implements Filter 
{
    private BlockingQueue<String> inputQueue;
    private BlockingQueue<String> outputQueue;

    public LinkRemover (BlockingQueue<String> inputQueue, BlockingQueue<String> outputQueue)
    {
        this.inputQueue=inputQueue;
        this.outputQueue =outputQueue;
    }

    public void run()
    {
        try
        {
            while(true)
            {
                String message =inputQueue.take();;
                if(message.equals("STOP")) break;

                outputQueue.put(message.replace("http", ""));
            }
            outputQueue.put("STOP");
        }
        catch(InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }
}

class SentimentAnalyzer implements Filter 
{
    private BlockingQueue<String> inputQueue;
    private BlockingQueue<String> outputQueue;

    public SentimentAnalyzer (BlockingQueue<String> inputQueue, BlockingQueue<String> outputQueue)
    {
        this.inputQueue=inputQueue;
        this.outputQueue=outputQueue;
    }

    public void run()
    {
        try
        {
            while(true)
            {
                String message =inputQueue.take();
                if(message.equals("STOP")) break;
                
                String[] parts = message.split(", ");
                if (parts.length >= 3 && !parts[2].isEmpty()) 
                {
                    String reviewedText = parts[2];
                    int upper = 0, lower = 0;

                    for (char c : reviewedText.toCharArray()) 
                    {
                        if (Character.isUpperCase(c)) upper++;
                        else if (Character.isLowerCase(c)) lower++;
                    }

                    if (upper > lower) 
                    {
                        parts[2] += "+";
                    } 
                    else if (lower > upper) 
                    {
                        parts[2] += "-";
                    } 
                    else 
                    {
                        parts[2] += "=";
                    }
                }

                outputQueue.put(String.join(", ", parts));
            }
            outputQueue.put("STOP");
        }
        catch (InterruptedException e) 
        {
            Thread.currentThread().interrupt();
        }
    }
}

class WriterFilter implements Filter
{
    private BlockingQueue<String> inputQueue;
    private String outputFile;

    public WriterFilter(BlockingQueue<String> inputQueue, String outputFile)
    {
        this.inputQueue=inputQueue;
        this.outputFile=outputFile;
    }

    public void run()
    {
        try(BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile)))
        {
            while(true)
            {
                String message=inputQueue.take();
                if(message.equals("STOP")) break;

                writer.write(message);
                writer.newLine();
            }
        }
        catch (IOException | InterruptedException e)
        {
            e.printStackTrace();
        }
    }
}

class ParallelPipeline 
{
    private final List<Thread> filterThreads= new ArrayList<>();

    public void executePipeline(String inputFile, String outputFile) 
    {
        BlockingQueue<String> queue1 = new LinkedBlockingQueue<>();
        BlockingQueue<String> queue2 = new LinkedBlockingQueue<>();
        BlockingQueue<String> queue3 = new LinkedBlockingQueue<>();
        BlockingQueue<String> queue4 = new LinkedBlockingQueue<>();
        BlockingQueue<String> queue5 = new LinkedBlockingQueue<>();
        BlockingQueue<String> queue6 = new LinkedBlockingQueue<>();
        BlockingQueue<String> queue7 = new LinkedBlockingQueue<>();

        HashSet<String> buyers=new HashSet<>(Arrays.asList
        (
            "John - Laptop", 
            "Mary - Phone",
            "Ann - BigMac",
            "Emanuel - Hyundai",
            "Razvan - Jas39",
            "Bob - Notebook",
            "Bogdan - Parrot",
            "Radu - Dog",
            "Lucian - Shoes",
            "Mihai - Coke",
            "Calin - Pants",
            "Stefan - Pen",
            "Toni - Guitar",
            "Luca - Football",
            "Andrei - Car",
            "Flavius - Shirt",
            "Marian - Outlet",
            "Peter - Tractor",
            "Piedone - Shawarma",
            "Matei - DVD",
            "Vasile - Wine",
            "Marioara - Cupcake",
            "Ghita - Toolbox",
            "Miriam - Mask",
            "Alex - MacBook",
            "Nicu - Sandwich",
            "Laura - Fish",
            "Sebastian - Flower",
            "Daniel - Bonsai",
            "Terry - Silver"
        ));

        ReaderFilter readerFilter = new ReaderFilter(inputFile, queue1);
        BuyerFilter buyerFilter = new BuyerFilter(buyers, queue1, queue2);
        ProfanityFilter profanityFilter = new ProfanityFilter(queue2, queue3);
        PoliticalFilter politicalFilter = new PoliticalFilter(queue3, queue4);
        ImageResizer imageResizer = new ImageResizer(queue4, queue5);
        LinkRemover linkRemover = new LinkRemover(queue5, queue6);
        SentimentAnalyzer sentimentAnalyzer = new SentimentAnalyzer(queue6, queue7);
        WriterFilter writerFilter = new WriterFilter(queue7, outputFile);

        startFilterThread(readerFilter);
        startFilterThread(buyerFilter);
        startFilterThread(profanityFilter);
        startFilterThread(politicalFilter);
        startFilterThread(imageResizer);
        startFilterThread(linkRemover);
        startFilterThread(sentimentAnalyzer);
        startFilterThread(writerFilter);

        for(Thread thread : filterThreads)
        {
            try
            {
                thread.join();
            }
            catch(InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void startFilterThread(Runnable filter)
    {
        Thread thread = new Thread(filter);
        thread.start();
        filterThreads.add(thread);
    }
}

public class Pipes_filters_Parallel 
{
    public static void main(String[] args) 
    {
        String inputFile = "input.txt";
        String outputFile = "output.txt";

        long startTime = System.currentTimeMillis();

        ParallelPipeline parallelPipeline = new ParallelPipeline();
        parallelPipeline.executePipeline(inputFile, outputFile);

        long endTime = System.currentTimeMillis();

        System.out.println("Pipeline Execution Time: "+(endTime-startTime)+" ms");
    }
}
