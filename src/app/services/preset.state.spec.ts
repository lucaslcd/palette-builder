import { TestBed } from '@angular/core/testing';

import { PresetState } from './preset.state';

describe('PresetState', () => {
  let service: PresetState;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(PresetState);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
