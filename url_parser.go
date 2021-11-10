package godata

import (
	"context"
	"fmt"
	"net/url"
	"strings"
)

func NewGoDataRequest() *GoDataRequest {
	return &GoDataRequest{}
}

// Parse a request from the HTTP server and format it into a GoDaataRequest type
// to be passed to a provider to produce a result.
func ParseRequest(ctx context.Context, path string, query url.Values, lenient bool) (*GoDataRequest, error) {
	r := &GoDataRequest{
		RequestKind: RequestKindUnknown,
	}

	if err := r.ParseUrlPath(path); err != nil {
		return nil, err
	}
	if err := r.ParseUrlQuery(ctx, query, lenient); err != nil {
		return nil, err
	}
	return r, nil
}

// Compare a request to a given service, and validate the semantics and update
// the request with semantics included
func (r *GoDataRequest) SemanticizeRequest(service *GoDataService) error {

	// if request kind is a resource
	for segment := r.FirstSegment; segment != nil; segment = segment.Next {
		err := SemanticizePathSegment(segment, service)
		if err != nil {
			return err
		}
	}

	switch r.LastSegment.SemanticReference.(type) {
	case *GoDataEntitySet:
		entitySet := r.LastSegment.SemanticReference.(*GoDataEntitySet)
		entityType, err := service.LookupEntityType(entitySet.EntityType)
		if err != nil {
			return err
		}
		err = SemanticizeFilterQuery(r.Query.Filter, service, entityType)
		if err != nil {
			return err
		}
		err = SemanticizeExpandQuery(r.Query.Expand, service, entityType)
		if err != nil {
			return err
		}
		err = SemanticizeSelectQuery(r.Query.Select, service, entityType)
		if err != nil {
			return err
		}
		err = SemanticizeOrderByQuery(r.Query.OrderBy, service, entityType)
		if err != nil {
			return err
		}
		// TODO: disallow invalid query params
	case *GoDataEntityType:
		entityType := r.LastSegment.SemanticReference.(*GoDataEntityType)
		if err := SemanticizeExpandQuery(r.Query.Expand, service, entityType); err != nil {
			return err
		}
		if err := SemanticizeSelectQuery(r.Query.Select, service, entityType); err != nil {
			return err
		}
	}

	if r.LastSegment.SemanticType == SemanticTypeMetadata {
		r.RequestKind = RequestKindMetadata
	} else if r.LastSegment.SemanticType == SemanticTypeRef {
		r.RequestKind = RequestKindRef
	} else if r.LastSegment.SemanticType == SemanticTypeEntitySet {
		if r.LastSegment.Identifier == nil {
			r.RequestKind = RequestKindCollection
		} else {
			r.RequestKind = RequestKindEntity
		}
	} else if r.LastSegment.SemanticType == SemanticTypeCount {
		r.RequestKind = RequestKindCount
	} else if r.FirstSegment == nil && r.LastSegment == nil {
		r.RequestKind = RequestKindService
	}

	return nil
}

func (r *GoDataRequest) ParseUrlPath(path string) error {
	parts := strings.Split(path, "/")
	r.FirstSegment = &GoDataSegment{
		RawValue:   parts[0],
		Name:       ParseName(parts[0]),
		Identifier: ParseIdentifiers(parts[0]),
	}
	currSegment := r.FirstSegment
	for _, v := range parts[1:] {
		temp := &GoDataSegment{
			RawValue:   v,
			Name:       ParseName(v),
			Identifier: ParseIdentifiers(v),
			Prev:       currSegment,
		}
		currSegment.Next = temp
		currSegment = temp
	}
	r.LastSegment = currSegment

	return nil
}

func SemanticizePathSegment(segment *GoDataSegment, service *GoDataService) error {
	var err error

	if segment.RawValue == "$metadata" {
		if segment.Next != nil || segment.Prev != nil {
			return BadRequestError("A metadata segment must be alone.")
		}

		segment.SemanticType = SemanticTypeMetadata
		segment.SemanticReference = service.Metadata
		return nil
	}

	if segment.RawValue == "$ref" {
		// this is a ref segment
		if segment.Next != nil {
			return BadRequestError("A $ref segment must be last.")
		}
		if segment.Prev == nil {
			return BadRequestError("A $ref segment must be preceded by something.")
		}

		segment.SemanticType = SemanticTypeRef
		segment.SemanticReference = segment.Prev
		return nil
	}

	if segment.RawValue == "$count" {
		// this is a ref segment
		if segment.Next != nil {
			return BadRequestError("A $count segment must be last.")
		}
		if segment.Prev == nil {
			return BadRequestError("A $count segment must be preceded by something.")
		}

		segment.SemanticType = SemanticTypeCount
		segment.SemanticReference = segment.Prev
		return nil
	}

	if _, ok := service.EntitySetLookup[segment.Name]; ok {
		// this is an entity set
		segment.SemanticType = SemanticTypeEntitySet
		segment.SemanticReference, err = service.LookupEntitySet(segment.Name)
		if err != nil {
			return err
		}

		if segment.Prev == nil {
			// this is the first segment
			if segment.Next == nil {
				// this is the only segment
				return nil
			} else {
				// there is at least one more segment
				if segment.Identifier != nil {
					return BadRequestError("An entity set must be the last segment.")
				}
				// if it has an identifier, it is allowed
				return nil
			}
		} else if segment.Next == nil {
			// this is the last segment in a sequence of more than one
			return nil
		} else {
			// this is a middle segment
			if segment.Identifier != nil {
				return BadRequestError("An entity set must be the last segment.")
			}
			// if it has an identifier, it is allowed
			return nil
		}
	}

	if segment.Prev != nil && segment.Prev.SemanticType == SemanticTypeEntitySet {
		// previous segment was an entity set
		semanticRef := segment.Prev.SemanticReference.(*GoDataEntitySet)

		entity, err := service.LookupEntityType(semanticRef.EntityType)

		if err != nil {
			return err
		}

		for _, p := range entity.Properties {
			if p.Name == segment.Name {
				segment.SemanticType = SemanticTypeProperty
				segment.SemanticReference = p
				return nil
			}
		}

		return BadRequestError("A valid entity property must follow entity set.")
	}

	return BadRequestError("Invalid segment " + segment.RawValue)
}

var supportedOdataKeywords = map[string]bool{
	"$filter":      true,
	"$apply":       true,
	"$expand":      true,
	"$select":      true,
	"$orderby":     true,
	"$top":         true,
	"$skip":        true,
	"$count":       true,
	"$inlinecount": true,
	"$search":      true,
	"$format":      true,
	"at":           true,
	"tags":         true,
}

// If lenient is true, the following logic is applied. If lenient is false, return an error:
// - Allow duplicate ODATA keywords in the URL query.
// - Ignore unknown ODATA keywords in the URL query.
// - Allow extraneous comma as the last character in a list of function arguments.
func (r *GoDataRequest) ParseUrlQuery(ctx context.Context, query url.Values, lenient bool) error {
	if !lenient {
		// Validate each query parameter is a valid ODATA keyword.
		for key, val := range query {
			if _, ok := supportedOdataKeywords[key]; !ok {
				return BadRequestError(fmt.Sprintf("Query parameter '%s' is not supported", key)).
					SetCause(&UnsupportedQueryParameterError{key})
			}
			if len(val) > 1 {
				return BadRequestError(fmt.Sprintf("Query parameter '%s' cannot be specified more than once", key)).
					SetCause(&DuplicateQueryParameterError{key})
			}
		}
	}
	filter := query.Get("$filter")
	at := query.Get("at")
	apply := query.Get("$apply")
	expand := query.Get("$expand")
	sel := query.Get("$select")
	orderby := query.Get("$orderby")
	top := query.Get("$top")
	skip := query.Get("$skip")
	count := query.Get("$count")
	inlinecount := query.Get("$inlinecount")
	search := query.Get("$search")
	format := query.Get("$format")

	result := &GoDataQuery{}

	var err error = nil
	if filter != "" {
		result.Filter, err = ParseFilterString(ctx, filter)
	}
	if err != nil {
		return err
	}
	if at != "" {
		result.At, err = ParseFilterString(ctx, at)
	}
	if err != nil {
		return err
	}
	if at != "" {
		result.At, err = ParseFilterString(ctx, at)
	}
	if err != nil {
		return err
	}
	if apply != "" {
		result.Apply, err = ParseApplyString(ctx, apply)
	}
	if err != nil {
		return err
	}
	if expand != "" {
		result.Expand, err = ParseExpandString(ctx, expand)
	}
	if err != nil {
		return err
	}
	if sel != "" {
		result.Select, err = ParseSelectString(ctx, sel)
	}
	if err != nil {
		return err
	}
	if orderby != "" {
		result.OrderBy, err = ParseOrderByString(ctx, orderby)
	}
	if err != nil {
		return err
	}
	if top != "" {
		result.Top, err = ParseTopString(ctx, top)
	}
	if err != nil {
		return err
	}
	if skip != "" {
		result.Skip, err = ParseSkipString(ctx, skip)
	}
	if err != nil {
		return err
	}
	if count != "" {
		result.Count, err = ParseCountString(ctx, count)
	}
	if err != nil {
		return err
	}
	if inlinecount != "" {
		result.InlineCount, err = ParseInlineCountString(ctx, inlinecount)
	}
	if err != nil {
		return err
	}
	if search != "" {
		result.Search, err = ParseSearchString(ctx, search)
	}
	if err != nil {
		return err
	}
	if format != "" {
		err = NotImplementedError("Format is not supported")
	}
	if err != nil {
		return err
	}
	r.Query = result
	return err
}

func ParseIdentifiers(segment string) *GoDataIdentifier {
	if !(strings.Contains(segment, "(") && strings.Contains(segment, ")")) {
		return nil
	}

	rawIds := segment[strings.LastIndex(segment, "(")+1 : strings.LastIndex(segment, ")")]
	parts := strings.Split(rawIds, ",")

	result := make(GoDataIdentifier)

	for _, v := range parts {
		if strings.Contains(v, "=") {
			split := strings.SplitN(v, "=", 2)
			result[split[0]] = split[1]
		} else {
			result[v] = ""
		}
	}

	return &result
}

func ParseName(segment string) string {
	if strings.Contains(segment, "(") {
		return segment[:strings.LastIndex(segment, "(")]
	} else {
		return segment
	}
}
