import java.util.*;
import java.util.concurrent.*;

interface Filter
{
    public void process(String message, BlockingQueue<String> queue) throws InterruptedException;
}

class BuyerFilter implements Filter
{
    private HashSet<String> buyers;

    public BuyerFilter(HashSet<String> buyers)
    {
        this.buyers=buyers;
    }

    public void process(String message, BlockingQueue<String> queue) throws InterruptedException
    {
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
    public void process(String message, BlockingQueue<String> queue) throws InterruptedException
    {
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
    public void process(String message, BlockingQueue<String> queue) throws InterruptedException
    {
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
    public void process(String message, BlockingQueue<String> queue) throws InterruptedException
    {
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
    public void process(String message, BlockingQueue<String> queue) throws InterruptedException
    {
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
    public void process(String message, BlockingQueue<String> queue) throws InterruptedException
    {
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

class FilterWorker implements Runnable
{
    private final Filter filter;
    private final BlockingQueue<String> inputQueue;
    private final BlockingQueue<String> outputQueue;

    public FilterWorker(Filter filter, BlockingQueue<String> inputQueue, BlockingQueue<String> outputQueue)
    {
        this.filter = filter;
        this.inputQueue=inputQueue;
        this.outputQueue= outputQueue;
    }

    public void run()
    {
        try
        {
            while(true)
            {
                String message = inputQueue.take();
                filter.process(message, outputQueue);
                if(message.equals("STOP")) break;
            }
        }
        catch(InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }
}

class ParallelPipeline
{
    private final List<Filter> filters;
    private final BlockingQueue<String> queue1 = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> queue2 = new LinkedBlockingQueue<>();

    public ParallelPipeline()
    {
        filters = List.of(
            new BuyerFilter(new HashSet<>(Set.of("John - Laptop", "Mary - Phone", "Ann - BigMac"))),
            new ProfanityFilter(),
            new PoliticalFilter(),
            new ImageResizer(), 
            new LinkRemover(),
            new SentimentAnalyzer()
        );
    }

    public void executePipeline(List<String> messages)
    {
        for( String message : messages )
        {
            queue1.add(message);
        }
        queue1.add("STOP");

        BlockingQueue<String> inputQueue = queue1;
        BlockingQueue<String> outputQueue = queue2;

        for(Filter filter : filters)
        {
            BlockingQueue<String> finalInput= inputQueue;
            BlockingQueue<String> finalOutput = outputQueue;

            Thread filterThread = new Thread(() -> {
                try {
                    filter.process(finalInput, finalOutput);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            filterThread.start();

            inputQueue=outputQueue;
            if(outputQueue==queue1)
            {
                outputQueue=queue2;
            }
            else
            {
                outputQueue=queue1;
            }

            try {
                filterThread.join(); // Ensure current filter completes before starting next
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            while(true)
            {
                try
                {
                    String result = inputQueue.take();
                    if(result.equals("STOP")) break;
                    System.out.println("Processed Message "+result);
                }
                catch(InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
            }
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

        ParallelPipeline ppipeline = new ParallelPipeline();
        ppipeline.executePipeline(messages);
    }
}