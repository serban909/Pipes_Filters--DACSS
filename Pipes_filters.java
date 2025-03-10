import java.util.*;

interface Filter
{
    List<String> process(List<String> messages);
}

class BuyerFilter implements Filter
{
    private HashSet<String> buyers;

    public BuyerFilter(HashSet<String> buyers)
    {
        this.buyers=buyers;
    }

    public List<String> process(List<String> messages)
    {
        List<String> result=new ArrayList<>();
        for(String message:messages)
        {
            String[] parts=message.split(", ");
            if(parts.length>=2 && buyers.contains(parts[0]+" - "+parts[1]))
            {
                result.add(message);
            }
        }

        return result;
    }
}

class ProfanityFilter implements Filter
{
    public List<String> process(List<String> messages)
    {
        List<String> result=new ArrayList<>();
        for(String message:messages)
        {
            if(!message.contains("@#$%"))
            {
                result.add(message);
            }
        }

        return result;
    }
}

class PoliticalFilter implements Filter
{
    public List<String> process(List<String> messages)
    {
        List<String> result=new ArrayList<>();
        for(String msg : messages)
        {
            if(!msg.contains("---") && !msg.contains("+++"))
            {
                result.add(msg);
            }
        }

        return result;
    }
}

class ImageResizer implements Filter
{
    public List<String> process(List<String> messages)
    {
        List<String> result =new ArrayList<>();
        for(String msg : messages)
        {
            String[] parts=msg.split(", ");
            if(parts.length==4)
            {
                parts[3]=parts[3].toLowerCase();
                result.add(String.join(", ", parts));
            }
            else
            {
                result.add(msg);
            }
        }

        return result;
    }
}

class LinkRemover implements Filter
{
    public List<String> process(List<String> messages)
    {
        List<String> result=new ArrayList<>();
        for(String msg : messages)
        {
            result.add(msg.replace("http", ""));
        }

        return result;
    }
}

class SentimentAnalyzer implements Filter
{
    public List<String> process(List<String> messages)
    {
        List<String> result=new ArrayList<>();
        for(String msg : messages)
        {
            String[] parts=msg.split(", ");
            if(parts.length>=3)
            {
                String reviewText=parts[2];
                int upperCase=0;
                int lowerCase=0;

                for(char c:reviewText.toCharArray())
                {
                    if(Character.isUpperCase(c))
                    {
                        upperCase++;
                    }
                    else if(Character.isLowerCase(c))
                    {
                        lowerCase++;
                    }
                }

                if(upperCase>lowerCase)
                {
                    parts[2]+="+";
                }
                else if(upperCase<lowerCase)
                {
                    parts[2]+="-";
                }
                else
                {
                    parts[2]+="=";
                }

                result.add(String.join(", ", parts));
            }
            else
            {
                result.add(msg);
            }
        }

        return result;
    }
}

class Pipeline
{
    private List<Filter> filters=new ArrayList<>();

    public void addFilter(Filter filter)
    {
        filters.add(filter);
    }

    public List<String> process(List<String> messages)
    {
        for(Filter filter : filters)
        {
            messages=filter.process(messages);
        }

        return messages;
    }
}

public class Pipes_filters
{
    public static void main(String[] args) 
    {
        HashSet<String> buyers=new HashSet<>(Arrays.asList("John - Laptop", "Mary - Phone", "Ann - BigMac"));

        List<String> messages= Arrays.asList(
            "John, Laptop, httpok, PICTURE",
            "Mary, Phone, @#$%), IMAGE",
            "Peter, Phone, GREAT, AloToFpiCtureS",
            "Ann, BigMac, So GOOD, Image"
        );

        Pipeline pipeline=new Pipeline();
        pipeline.addFilter(new BuyerFilter(buyers));
        pipeline.addFilter(new ProfanityFilter());
        pipeline.addFilter(new PoliticalFilter());
        pipeline.addFilter(new ImageResizer());
        pipeline.addFilter(new LinkRemover());
        pipeline.addFilter(new SentimentAnalyzer());

        List<String> processedMessages =pipeline.process(messages);
        for(String msg : processedMessages)
        {
            System.out.println(msg);
        }
    }
}