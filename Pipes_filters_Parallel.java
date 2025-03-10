import java.util.*;
import java.util.concurrent.*;

interface Filter
{
    public void process(BlockingQueue<String> queue) throws InterruptedException;
}

class BuyerFilter implements Filter
{
    private HashSet<String> buyers;

    public BuyerFilter(HashSet<String> buyers)
    {
        this.buyers=buyers;
    }

    public void process(BlockingQueue<String> queue) throws InterruptedException
    {
        String message=queue.take();
        if(message.equals("STOP"))
        {
            queue.put("STOP");
            return;
        }

        String[] parts=message.split(", ");
        if(parts.length>=2 && buyers.contains(parts[0]+" - "+parts[1]))
        {
            queue.put(message);
        }

        Thread.sleep(100);
    }
}

class ProfanityFilter implements Filter
{
    public void process(BlockingQueue<String> queue) throws InterruptedException
    {
        String message = queue.take();
        if(message.equals("STOP"))
        {
            queue.put("STOP");
            return;
        }
        if(!message.contains("@#$%"))
        {
            queue.put(message);
        }

        Thread.sleep(100);
    }
}

class PoliticalFilter implements Filter
{
    public void process(BlockingQueue<String> queue) throws InterruptedException
    {
        String message=queue.take();

        if(message.equals("STOP"))
        {
            queue.put("STOP");
            return;
        }

        if(!message.contains("+++") && message.contains("---"))
        {
            queue.put(message);
        }

        Thread.sleep(100);
    }
}

class ImageResizer implements Filter
{
    public void process(BlockingQueue<String> queue) throws InterruptedException
    {
        String message = queue.take();
        if(message.equals("STOP"))
        {
            queue.put("STOP");
            return;
        }

        String[] parts=message.split(", ");
        if(parts.length==4)
        {
            parts[3] = parts[3].toLowerCase();
        }

        queue.put(String.join(", ", parts));
        Thread.sleep(100);
    }
}

class LinkRemover implements Filter
{
    public void process(BlockingQueue<String> queue) throws InterruptedException
    {
        String message = queue.take();
        if(message.equals("STOP"))
        {
            queue.put("STOP");
            return;
        }

        queue.put(message.replace("http", ""));
        Thread.sleep(100);
    }
}

class SentimentAnalyzer implements Filter
{
    public void process(BlockingQueue<String> queue) throws InterruptedException
    {
        String message= queue.take();
        if(message.equals("STOP"))
        {
            queue.put("STOP");
            return;
        }

        String[] parts=message.split(", ");
        if(parts.length>=3)
        {
            String reviewedText = parts[2];
            int upper=0;
            int lower=0;

            for(char c:reviewedText.toCharArray())
            {
                if(Character.isUpperCase(c)) upper++;
                else if(Character.isLowerCase(c)) lower++;
            }

            if(upper>lower)
            {
                parts[2]+="+";
            }
            else if(lower>upper)
            {
                parts[2]+="-";
            }
            else
            {
                parts[2]+="=";
            }
        }

        queue.put(String.join(", ", parts));
        Thread.sleep(100);
    }
}

class MessageOutput implements Filter
{
    public void process(BlockingQueue<String> queue) throws InterruptedException
    {
        String message = queue.take();

        if(message.equals("STOP")) return;
        
        System.out.println("Processed Message: "+ message);
    }
}

class FilterWorker implements Runnable
{
    private final Filter filter;
    private final BlockingQueue<String> inputQueue;
    private final BlockingQueue<String> outputQueue;

    public FilterWorker(Filter filter, BlockingQueue<String> inputQueue, BlockingQueue<String> outputQueue)
    {
        this.filter = filter;
        this.inputQueue=inputQueue;
        this.outputQueue=outputQueue;
    }

    public void run()
    {
        filter.process(inputQueue, outputQueue);
    }
}

class ParallelPipeline
{
    private BlockingQueue<String> bqueue1=new LinkedBlockingQueue<>();
    private BlockingQueue<String> bqueue2=new LinkedBlockingQueue<>();

    public void executePipeline(String[] messages)
    {
        for(String message:messages)
        {
            bqueue1.add(message);
        }
        bqueue1.add("STOP");

        BuyerFilter buyerFilter = new BuyerFilter(new HashSet<>(Set.of("John - Laptop", "Mary - Phone", "Ann - BigMac")));
        ProfanityFilter profanityFilter = new ProfanityFilter();
        PoliticalFilter politicalFilter = new PoliticalFilter();
        ImageResizer imageResizer = new ImageResizer();
        LinkRemover linkRemover = new LinkRemover();
        SentimentAnalyzer sentimentAnalyzer = new SentimentAnalyzer();
        MessageOutput messageOutput = new MessageOutput();

        Thread thread1 = new Thread(new FilterWorker(politicalFilter, bqueue2, bqueue1));
        Thread thread2 = new Thread(new FilterWorker(politicalFilter, bqueue2, bqueue1));

        thread1.start();
        thread2.start();

        try
        {
            thread1.join();
            thread2.join();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }

        while(!bqueue2.isEmpty())
        {
            System.out.println(bqueue2.poll());
        }
    }
}

public class Pipes_filters_Parallel
{
    public static void main(String[] args) 
    {
        String[] messages = 
        {   
            "John, Laptop, ok, PICTURE",
            "Mary, Phone, @#$%), IMAGE",
            "Peter, Phone, GREAT, AloToFpiCtureS",
            "Ann, BigMac, So GOOD, Image"
        };

        ParallelPipeline pipeline = new ParallelPipeline();
        pipeline.executePipeline(messages);
    }
}