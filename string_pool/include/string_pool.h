#include <string>
#include <memory>
#include <map>

class flyweight_string
{

private:

    std::string _data;

public:

    flyweight_string(
        std::string const &str);

public:

    std::string get_data() const;
};

class flyweight_string_pool
{
    
private:

    std::map<std::string, std::shared_ptr<flyweight_string>> _flyweight_pool;

public:

    static flyweight_string_pool *get_instance();

public:

    std::shared_ptr<flyweight_string> convert_to_flyweight(const std::string &str);

    void consolidate();

private:

    flyweight_string_pool();
    
public:

    flyweight_string_pool(
        flyweight_string_pool const &) = delete;
    
    flyweight_string_pool(
        flyweight_string_pool &&) = delete;

};