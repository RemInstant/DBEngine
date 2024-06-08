#include "../include/flyweight_string_pool.h"

#include <iostream>

flyweight_string::flyweight_string(
    std::string const &str):
        _data(str)
{ }

std::string flyweight_string::get_data() const
{
    return _data;
}


flyweight_string_pool *flyweight_string_pool::flyweight_string_pool::get_instance()
{
    static auto *pool = new flyweight_string_pool();
    return pool; 
}

std::shared_ptr<flyweight_string> flyweight_string_pool::make_flyweight(
    std::string const & str)
{
    auto it = _flyweight_pool.find(str);
    if (it != _flyweight_pool.end())
    {
        return it->second;
    }

    std::shared_ptr<flyweight_string> flyweight = std::make_shared<flyweight_string>(str);
    _flyweight_pool[str] = flyweight;
    
    return flyweight;
}

void flyweight_string_pool::consolidate()
{
    for (auto iter = _flyweight_pool.begin(); iter != _flyweight_pool.end();)
    {
        if (iter->second.use_count() == 1)
        {
            iter = _flyweight_pool.erase(iter);
        }
        else
        {
            ++iter;
        }
    }
}

flyweight_string_pool::flyweight_string_pool() { }
