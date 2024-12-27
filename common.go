package optimistic

import (
	"fmt"
	"gorm.io/gorm/schema"
	"reflect"
	"strings"
)

type contextValue struct {
	Current                       any
	PrimaryKey                    any
	Version                       Version
	PrimaryKeyField, VersionField *schema.Field
}

// ExtractField extracts the values of a specified field (including nested fields via "dot" notation)
// from a slice of structs or pointers to structs. It returns a slice of the specified field type.
func ExtractField[T any, F any](slice []T, fieldPath string) ([]F, error) {
	var result []F
	fields := strings.Split(fieldPath, ".") // Split the fieldPath into components

	for _, item := range slice {
		v := reflect.ValueOf(item)

		// Dereference pointer if necessary
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}

		// Traverse the nested fields
		for _, field := range fields {
			if v.Kind() == reflect.Struct {
				v = v.FieldByName(field)
			} else {
				return nil, fmt.Errorf("field %q is not a struct in the path %q", field, fieldPath)
			}

			if !v.IsValid() {
				return nil, fmt.Errorf("field %q not found in the path %q", field, fieldPath)
			}
		}

		// Check if the final field's type matches the desired output type
		fieldValue, ok := v.Interface().(F)
		if !ok {
			return nil, fmt.Errorf("field %q is not of type %T", fieldPath, *new(F))
		}

		result = append(result, fieldValue)
	}

	return result, nil
}
