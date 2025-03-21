package parsers

import (
	"github.com/influxdata/influxdb/models"
	"testing"
)

func TestGetSchemaId(t *testing.T) {
	fields := models.Fields{
		"temperature": 1.2,
		"humidity":    "asd",
	}
	tags := models.Tags{
		{Key: []byte("location"), Value: []byte("New York")},
	}
	id1 := getSchemaId(fields, tags)

	fields = models.Fields{
		"temperature": "asd",
		"humidity":    "asd",
	}
	tags = models.Tags{
		{Key: []byte("location"), Value: []byte("New York")},
	}
	id2 := getSchemaId(fields, tags)
	if id2 == id1 {
		t.Fatalf(
			"Expected different schema IDs for different field types, got %d and %d", id1, id2)
	}

	fields = models.Fields{
		"temperature": 1.2,
	}
	tags = models.Tags{
		{Key: []byte("location"), Value: []byte("New York")},
		{[]byte("humidity"), []byte("asd")},
	}
	if getSchemaId(fields, tags) != id1 {
		t.Fatalf("Expected same schema ID for different fields + tag order, got %d and %d",
			id1, getSchemaId(fields, tags))
	}

	fields = models.Fields{
		"temperature": 1.2,
	}
	tags = models.Tags{
		{[]byte("humidity"), []byte("asd")},
		{Key: []byte("location"), Value: []byte("New York")},
	}
	if getSchemaId(fields, tags) != id1 {
		t.Fatalf("Expected same schema ID for different fields + tag order, got %d and %d",
			id1, getSchemaId(fields, tags))
	}
}
