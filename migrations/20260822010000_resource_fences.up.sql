CREATE TABLE IF NOT EXISTS shardline_resource_fences (
    domain TEXT NOT NULL CHECK (length(domain) > 0),
    resource TEXT NOT NULL CHECK (length(resource) > 0),
    epoch BIGINT NOT NULL CHECK (epoch > 0),
    PRIMARY KEY (domain, resource)
);
