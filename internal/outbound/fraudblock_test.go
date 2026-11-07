package outbound

import (
	"encoding/json"
	"testing"
)

func TestFraudCheckDeflateSecondsIsOnlySentWhenRecorded(t *testing.T) {
	seconds := 5400
	withCut, err := json.Marshal(FraudCheckResult{Email: "maker1@example.com", DeflateSeconds: &seconds})
	if err != nil {
		t.Fatal(err)
	}
	if string(withCut) != `{"email":"maker1@example.com","slack_id":null,"trust_score":null,"justification":null,"deflate_seconds":5400}` {
		t.Fatalf("with a cut: %s", withCut)
	}
	without, err := json.Marshal(FraudCheckResult{Email: "maker2@example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if string(without) != `{"email":"maker2@example.com","slack_id":null,"trust_score":null,"justification":null}` {
		t.Fatalf("an older review keeps the old shape: %s", without)
	}
}
