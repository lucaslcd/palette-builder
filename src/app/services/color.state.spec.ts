import { TestBed } from '@angular/core/testing';

import { ColorState } from './color.state';

describe('ColorState', () => {
  let service: ColorState;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(ColorState);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
