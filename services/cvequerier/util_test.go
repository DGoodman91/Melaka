package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadFromENV_Returns_Value_When_Env_Is_Set(t *testing.T) {

	key, value, defaultVal := "MY_KEY", "MY_VAL", "DEFAULT_VAL"
	os.Setenv(key, value)

	assert.Equal(t, readFromENV(key, defaultVal), value)
}

func TestReadFromENV_Returns_Default_Value_When_Env_Is_Not_Set(t *testing.T) {

	key, defaultVal := "MY_KEY2", "DEFAULT_VAL"

	assert.Equal(t, readFromENV(key, defaultVal), defaultVal)

}
