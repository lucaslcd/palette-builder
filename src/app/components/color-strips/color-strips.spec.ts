import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ColorStrips } from './color-strips';

describe('ColorStrips', () => {
  let component: ColorStrips;
  let fixture: ComponentFixture<ColorStrips>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ColorStrips],
    }).compileComponents();

    fixture = TestBed.createComponent(ColorStrips);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
